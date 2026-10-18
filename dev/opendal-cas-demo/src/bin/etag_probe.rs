//! ETag 语义探针：验证 S3/MinIO 的 ETag 是不是「内容的 MD5」、会不会随版本单调变化。
//!
//! 目的是给「ETag 不能当 fencing token」这个结论留下一份可复现的证据：
//! 如果同一个 key 在不同版本上可以出现相同 ETag，那 ETag 就无法回答
//! 「我读到的这一版有没有被别人改过」之外的任何问题——尤其是「这是第几版」。
//!
//! 跑法（需 MinIO 在 127.0.0.1:9000，bucket 已存在）：
//!   cargo run --bin etag_probe

use opendal::services::S3;
use opendal::Operator;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// 连接 MinIO（S3 兼容）。默认值对齐 GreptimeDB 仓库 tests-integration/fixtures 的 docker-compose。
fn build_operator() -> opendal::Result<Operator> {
    let endpoint = env_or("MINIO_ENDPOINT", "http://127.0.0.1:9000");
    let region = env_or("MINIO_REGION", "us-west-2");
    let ak = env_or("MINIO_ACCESS_KEY_ID", "superpower_ci_user");
    let sk = env_or("MINIO_SECRET_ACCESS_KEY", "superpower_password");
    let bucket = env_or("MINIO_BUCKET", "greptime");

    Operator::new(
        S3::default()
            .endpoint(&endpoint)
            .region(&region)
            .access_key_id(&ak)
            .secret_access_key(&sk)
            .bucket(&bucket)
            .root("/"),
    )
}

fn tag(meta: &opendal::Metadata) -> String {
    meta.etag().unwrap_or("<no etag>").to_string()
}

#[tokio::main]
async fn main() -> Result<(), opendal::Error> {
    let op = build_operator()?;

    let key = "etag-probe/same-key";
    let key2 = "etag-probe/other-key";
    let a: Vec<u8> = b"hello-etag".to_vec();
    let b: Vec<u8> = b"world-etag".to_vec();

    println!("=== ETag 语义探针（MinIO）===\n");

    // [1] 首次写内容 A
    let e1 = tag(&op.write_with(key, a.clone()).await?);
    println!("[1] write(A)                etag = {e1}");

    // [2] 覆盖写同样的内容 A：写入确实发生了（版本/updated 变了），但内容没变
    let e2 = tag(&op.write_with(key, a.clone()).if_match(&e1).await?);
    println!(
        "[2] if_match(e1) → 再写 A   etag = {e2}   {}",
        if e2 == e1 {
            "与 e1 相同  ← 内容相同 ⇒ ETag 相同（写入发生了，ETag 却没变）"
        } else {
            "与 e1 不同"
        }
    );

    // [3] 覆盖写内容 B
    let e3 = tag(&op.write_with(key, b.clone()).if_match(&e2).await?);
    println!(
        "[3] if_match(e2) → B        etag = {e3}   {}",
        if e3 != e1 {
            "与 e1 不同  ← 内容变 ⇒ ETag 变"
        } else {
            "与 e1 相同"
        }
    );

    // [4] 内容回到 A：旧 ETag 会不会重现
    let e4 = tag(&op.write_with(key, a.clone()).if_match(&e3).await?);
    println!(
        "[4] if_match(e3) → 回到 A   etag = {e4}   {}",
        if e4 == e1 {
            "与 e1 相同  ← 旧 ETag 重现，不单调，无法用大小/先后关系做 fencing"
        } else {
            "与 e1 不同"
        }
    );

    // [5] 另一个 key 写同样的内容 A
    let e5 = tag(&op.write_with(key2, a.clone()).await?);
    println!(
        "[5] write(other-key, A)     etag = {e5}   {}",
        if e5 == e1 {
            "与 e1 相同  ← 与 key 无关，纯粹是内容摘要"
        } else {
            "与 e1 不同"
        }
    );

    // [6] 拿「属于内容 B 的旧 ETag」在内容已回到 A 之后再 CAS 一次
    match op.write_with(key, a.clone()).if_match(&e3).await {
        Ok(meta) => println!(
            "[6] if_match(e3 已过期)     竟然成功！etag = {}",
            tag(&meta)
        ),
        Err(e) => println!("[6] if_match(e3 已过期)     被拒：{}", e.kind()),
    }

    println!("\n=== 结论用 ===");
    println!("e1={e1}");
    println!("e2={e2}");
    println!("e3={e3}");
    println!("e4={e4}");
    println!("e5={e5}");
    println!("\n把 e1 和 `printf 'hello-etag' | md5sum` 对一下，就能确认 ETag 是不是内容 MD5。");

    Ok(())
}
