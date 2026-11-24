"""冲突裁决领域服务：维护同一记录主键在源端与目标端字段取值冲突的登记与裁决依据。

本模块在校验核对发现差异后自动创建冲突记录，支持按裁决方式（源端优先/目标端优先/人工裁决）
处理字段取值冲突，裁决完成后写入裁决日志和裁决产物文件。

批量裁决确认时产出「冲突裁决生效清单」：按所选批次的字段冲突明细写入实际采用值与对应
字段版本，交给下游（传输血缘→目标合并）消费。一轮提交内同一字段只生效一次；已生效条目
可核对采用值、裁决方式与目标运用的字段版本；提交时若预览版本与当前字段版本不一致
（如单项复核刚更正过目标值），以当前更正值生效并显式标记版本漂移，不会用预览值悄悄
盖掉更正。重发同一批次的确认不会产生重复生效写入。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ConflictResolveService(BaseModuleService):
    MODULE = "conflict_resolve"
    ENTITY = "冲突记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["冲突编号", "记录主键", "冲突字段", "源端取值", "目标取值", "裁决方式", "裁决依据", "裁决时间"]
    REQUIRED_FIELDS = ["冲突编号", "记录主键", "冲突字段"]
    STATUS_ORDER = ["草稿", "待裁决", "已裁决", "已挂起"]
    ACTION_RULES = {"提交裁决": "待裁决", "确认裁决": "已裁决", "标记挂起": "已挂起"}
    NEGATIVE_ACTIONS = ["标记挂起"]

    def create_conflict(self, batch_code: str, diff_count: int) -> dict[str, Any]:
        """基于校验发现的差异条数创建冲突记录，登记冲突字段与源端目标端取值对。"""
        created = []
        for i in range(min(diff_count, 5)):
            conflict_code = f"CONF-{batch_code}-{i + 1}"
            values = {
                "冲突编号": conflict_code,
                "记录主键": f"PK_{batch_code}_{i}",
                "冲突字段": f"field_{i}",
                "源端取值": f"src_val_{i}",
                "目标取值": f"tgt_val_{i}",
                "裁决方式": "待裁决",
                "裁决依据": "",
                "裁决时间": time.strftime("%Y-%m-%d", time.localtime(time.time())),
            }
            entry, _ = self.create_entry(values)
            if entry:
                entry["批次编号"] = batch_code
                entry["字段版本"] = 1
                entry["采用值"] = ""
                created.append(conflict_code)
        self._write_jsonl(
            self._data_dir("events") / "conflict_log.jsonl",
            [{"action": "create_conflicts", "batch_code": batch_code,
              "conflict_count": len(created), "at": time.time()}]
        )
        return {"batch_code": batch_code, "conflicts_created": len(created), "codes": created}

    def resolve_conflict(self, conflict_id: int, strategy: str = "source_first") -> dict[str, Any]:
        """执行冲突裁决：按指定策略裁决字段取值冲突并写入裁决产物文件。

        裁决策略选项：
        - source_first：源端取值优先，取源端值作为最终数据
        - target_first：目标端取值优先，取目标端值作为最终数据
        - manual_override：人工裁决，标记为已挂起等待人工介入
        """
        conflict_entry = self.get_entry(conflict_id)
        if conflict_entry is None:
            return {"error": f"冲突记录 {conflict_id} 不存在"}

        entry, msg = self.run_action(conflict_id, "提交裁决")
        if entry is None:
            return {"error": msg, "conflict_id": conflict_id}

        source_val = conflict_entry.get("源端取值", "")
        target_val = conflict_entry.get("目标取值", "")

        if strategy == "source_first":
            final_val = source_val
            basis = "源端优先：以源端系统数据为准"
            entry["裁决方式"] = "源端优先"
        elif strategy == "target_first":
            final_val = target_val
            basis = "目标端优先：以目标端系统数据为准"
            entry["裁决方式"] = "目标端优先"
        else:
            final_val = "PENDING_MANUAL"
            basis = "挂起待人工裁决"
            entry["裁决方式"] = "人工裁决"
            self.run_action(conflict_id, "标记挂起")

        entry["裁决依据"] = basis
        entry["裁决时间"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time()))
        entry["采用值"] = final_val
        entry.setdefault("字段版本", 1)

        if strategy != "manual_override":
            final_entry, _ = self.run_action(conflict_id, "确认裁决")

        # 写入裁决产物文件
        resolution_file = self._data_dir("resolutions") / f"{conflict_entry['冲突编号']}_resolution.jsonl"
        self._write_jsonl(resolution_file, [{
            "conflict_code": conflict_entry["冲突编号"],
            "strategy": strategy,
            "source_value": source_val,
            "target_value": target_val,
            "final_value": final_val,
            "basis": basis,
            "resolved_at": time.time(),
        }])

        return {
            "conflict_code": conflict_entry["冲突编号"],
            "strategy": strategy,
            "final_value": final_val,
            "basis": basis,
        }

    def batch_resolve(self, batch_code: str, strategy: str = "source_first") -> dict[str, Any]:
        """批量裁决同一批次的全部待裁决冲突记录。"""
        conflicts = [r for r in store.rows(self.MODULE)
                     if r.get("批次编号", "").startswith(batch_code[:8])
                     and r.get("status") == "待裁决"]
        resolved = 0
        for c in conflicts:
            result = self.resolve_conflict(c["id"], strategy)
            if "error" not in result:
                resolved += 1
        return {"batch_code": batch_code, "total_conflicts": len(conflicts), "resolved": resolved}

    # -- 单项复核与批量提交（冲突裁决生效清单） --

    def review_conflict(self, conflict_id: int, new_target_value: str) -> dict[str, Any]:
        """单项复核：更正单笔冲突的目标取值，字段版本递增并同步采用值。

        复核更正会推进字段版本；若该笔裁决方式为目标端优先，采用值跟随目标值更新。
        批量提交据此识别预览版本与当前版本的漂移，避免用过期预览值盖掉本次更正。
        """
        entry = self.get_entry(conflict_id)
        if entry is None:
            return {"error": f"冲突记录 {conflict_id} 不存在"}

        entry["目标取值"] = new_target_value
        entry["字段版本"] = int(entry.get("字段版本", 1)) + 1
        if entry.get("裁决方式") == "目标端优先":
            entry["采用值"] = new_target_value
            entry["裁决依据"] = "目标端优先：单项复核更正目标取值后同步采用值"

        self._write_jsonl(
            self._data_dir("events") / "conflict_log.jsonl",
            [{"action": "review_conflict", "conflict_code": entry.get("冲突编号"),
              "field_version": entry["字段版本"], "new_target_value": new_target_value,
              "at": time.time()}]
        )
        return {
            "conflict_code": entry.get("冲突编号"),
            "字段版本": entry["字段版本"],
            "采用值": entry.get("采用值", ""),
        }

    def prepare_batch_commit(self, batch_code: str) -> dict[str, Any]:
        """为所选批次准备一轮批量提交：汇总已裁决冲突的字段明细预览。

        预览逐笔记录采用值与当时的字段版本，供提交时比对版本漂移；
        同一（记录主键, 冲突字段）在一轮提交中只保留一笔，保证同个字段只生效一次。
        """
        candidates = [r for r in store.rows(self.MODULE)
                      if r.get("批次编号") == batch_code and r.get("status") == "已裁决"]
        preview = []
        seen_fields: set[tuple[str, str]] = set()
        for row in candidates:
            field_key = (str(row.get("记录主键", "")), str(row.get("冲突字段", "")))
            if field_key in seen_fields:
                continue
            seen_fields.add(field_key)
            preview.append({
                "conflict_code": row.get("冲突编号"),
                "record_key": field_key[0],
                "field": field_key[1],
                "adopted_value": row.get("采用值", ""),
                "adjudication": row.get("裁决方式", ""),
                "source_value": row.get("源端取值", ""),
                "target_value": row.get("目标取值", ""),
                "field_version": int(row.get("字段版本", 1)),
            })

        round_id = f"{batch_code}-R{int(time.time() * 1000)}"
        self._write_checkpoint(f"batch_commit_{batch_code}", {
            "round_id": round_id,
            "batch_code": batch_code,
            "status": "prepared",
            "preview": preview,
        })
        return {"round_id": round_id, "batch_code": batch_code,
                "count": len(preview), "entries": preview}

    def confirm_batch_commit(self, batch_code: str) -> dict[str, Any]:
        """确认已准备的批量提交，产出冲突裁决生效清单交给下游合并消费。

        提交时逐笔重读当前冲突记录：
        - 字段版本与预览不一致（如准备后走过单项复核）时，以当前更正值与当前版本生效，
          并显式标记 version_drift，不会用预览值悄悄盖掉更正；
        - 一轮提交内同一字段只生效一次；
        - 已生效过的（记录主键, 冲突字段）直接跳过，重发原批确认不会产生第二笔有效写入。
        """
        checkpoint = self._read_checkpoint(f"batch_commit_{batch_code}")
        if checkpoint is None or checkpoint.get("status") != "prepared":
            return {"error": f"批次 {batch_code} 没有待确认的批量提交", "batch_code": batch_code}

        effective_file = self._data_dir("effective_lists") / f"{batch_code}_effective.jsonl"
        existing = self._read_jsonl(effective_file)
        effective_keys = {(e.get("record_key"), e.get("field")) for e in existing}

        effective: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        drifted: list[dict[str, Any]] = []
        seen_fields: set[tuple[str, str]] = set()

        for preview in checkpoint.get("preview", []):
            field_key = (preview.get("record_key"), preview.get("field"))
            if field_key in seen_fields:
                skipped.append({**preview, "reason": "一轮提交内同个字段只能生效一次"})
                continue
            seen_fields.add(field_key)
            if field_key in effective_keys:
                skipped.append({**preview, "reason": "该字段已生效，重复确认不再写入"})
                continue

            row = next((r for r in store.rows(self.MODULE)
                        if r.get("冲突编号") == preview.get("conflict_code")
                        and r.get("批次编号") == batch_code), None)
            if row is None or row.get("status") != "已裁决":
                skipped.append({**preview, "reason": "冲突记录不存在或不在已裁决状态"})
                continue

            current_version = int(row.get("字段版本", 1))
            drift = current_version != int(preview.get("field_version", 0))
            # 竞态保护：采用值/裁决/版本一律取提交时的当前记录，预览仅用于漂移检测
            record = {
                "batch_code": batch_code,
                "round_id": checkpoint["round_id"],
                "conflict_code": row.get("冲突编号"),
                "record_key": preview.get("record_key"),
                "field": preview.get("field"),
                "adopted_value": row.get("采用值", ""),
                "adjudication": row.get("裁决方式", ""),
                "source_value": row.get("源端取值", ""),
                "target_value": row.get("目标取值", ""),
                "field_version": current_version,
                "preview_version": int(preview.get("field_version", 0)),
                "version_drift": drift,
                "effective_at": time.time(),
            }
            effective.append(record)
            effective_keys.add(field_key)
            if drift:
                drifted.append(record)

        if effective:
            self._write_jsonl(effective_file, effective)
        checkpoint["status"] = "confirmed"
        checkpoint["confirmed_at"] = time.time()
        self._write_checkpoint(f"batch_commit_{batch_code}", checkpoint)

        result: dict[str, Any] = {
            "batch_code": batch_code,
            "round_id": checkpoint["round_id"],
            "effective_count": len(effective),
            "effective": effective,
            "skipped": skipped,
            "drifted": drifted,
        }

        # 生效清单产出后自动驱动传输血缘消费
        try:
            tl_svc = self._safe_import("app.services.transfer_lineage", "TransferLineageService")()
            result["auto_lineage"] = tl_svc.consume_effective_list(batch_code)
        except Exception as e:
            result["auto_lineage"] = {"error": str(e)}

        return result

    def get_effective_list(self, batch_code: str) -> list[dict[str, Any]]:
        """读取批次已产出的冲突裁决生效清单，供核对采用值、裁决方式与字段版本。"""
        effective_file = self._data_dir("effective_lists") / f"{batch_code}_effective.jsonl"
        return self._read_jsonl(effective_file)