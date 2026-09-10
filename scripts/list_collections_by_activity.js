// quant_data 集合按「最后写入时间」排序（近似，用于 Compass 替代）。
//
// MongoDB 不保存集合级最后修改时间；近似方法：
//   1. 优先取每集合最后一篇文档的 updated_at / update_time / created_at 字段
//      （兼容 Date 与 "YYYY-MM-DD HH:MM:SS" / "YYYYMMDD" 字符串）；
//   2. 都没有则用 _id.getTimestamp()（ObjectId 前 4 字节 = 写入时间）。
//
// 用法:
//   docker exec -i quant-mongodb mongosh "mongodb://admin:changeme@localhost:27017/?authSource=admin" --quiet \
//     /dev/stdin < scripts/list_collections_by_activity.js
//   # 只看最近 N 天有写入的：
//   ... --eval 'load("/dev/stdin")' 之外可自行改 MAX_AGE_DAYS
const dbName = "quant_data";
const MAX_AGE_DAYS = 30; // 只显示最近 N 天内有写入的集合；null = 全部

function parseTs(v) {
  if (v === undefined || v === null) return null;
  if (v instanceof Date) return v;
  if (typeof v === "number") return new Date(v);
  if (typeof v === "string") {
    // "2026-09-10 11:28:46" 或 "20260910 11:28:46"
    let m = v.match(/^(\d{4})-?(\d{2})-?(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2}))?/);
    if (m) return new Date(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +(m[6] || 0));
    // "202609101128" 或 "20260910"
    m = v.match(/^(\d{4})(\d{2})(\d{2})(\d{2})?(\d{2})?/);
    if (m) return new Date(+m[1], +m[2] - 1, +m[3], +(m[4] || 0), +(m[5] || 0));
  }
  return null;
}

const db = db.getSiblingDB(dbName);
const rows = [];
for (const name of db.getCollectionNames()) {
  if (name === "system.profile") continue;
  const coll = db.getCollection(name);
  let last = null;
  try {
    // ObjectId 单调递增，按 _id 降序取最新一篇；无 ObjectId 的集合自然序兜底
    const docs = coll
      .find(
        {},
        { _id: 1, updated_at: 1, update_time: 1, created_at: 1, ts: 1 }
      )
      .sort({ _id: -1 })
      .limit(1)
      .toArray();
    const doc = docs[0];
    if (!doc) continue;
    for (const f of ["updated_at", "update_time", "created_at", "ts"]) {
      const t = parseTs(doc[f]);
      if (t && t.getFullYear() >= 2000) { last = t; break; }
    }
    if (!last && doc._id && typeof doc._id.getTimestamp === "function") {
      last = doc._id.getTimestamp();
    }
  } catch (e) { /* 无读取权限/结构异常，跳过 */ }
  if (last) rows.push({ name, last });
}
rows.sort((a, b) => b.last - a.last);
const cutoff = MAX_AGE_DAYS === null ? null : new Date(Date.now() - MAX_AGE_DAYS * 86400e3);
print("collection\tlast_write\tdocs");
for (const r of rows) {
  if (cutoff && r.last < cutoff) continue;
  const n = db.getCollection(r.name).countDocuments({});
  const p2 = (s) => String(s).padStart(2, "0");
  print(
    r.name.padEnd(40) +
    r.last.getFullYear() + "-" + p2(r.last.getMonth() + 1) + "-" + p2(r.last.getDate()) + " " +
    p2(r.last.getHours()) + ":" + p2(r.last.getMinutes()) +
    "\t" + n
  );
}
