#[cfg(test)]
pub mod test {
    use std::sync::Arc;

    use reqwest::StatusCode;
    use serial_test::serial;

    use crate::utils::{clean_env, start_sepration_cluster};

    #[test]
    #[serial]
    fn api_router() {
        println!("test api_router begin ...");
        clean_env();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);
        let (_meta, data) = start_sepration_cluster(runtime, 1, 1, 1);
        let resp = data
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/write?db=public",
                "api_router,ta=a fa=1 1",
            )
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let resp = data
            .client
            .post(
                "http://127.0.0.1:8912/api/v1/write?db=public",
                "api_router,ta=a fa=1 1",
            )
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = data
            .client
            .post(
                "http://127.0.0.1:8902/api/v1/sql?db=public",
                "select * from api_router",
            )
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let resp = data
            .client
            .post(
                "http://127.0.0.1:8912/api/v1/sql?db=public",
                "select * from api_router",
            )
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.text().unwrap(),
            "time,ta,fa\n1970-01-01T00:00:00.000000001,a,1.0\n"
        );
    }
}
