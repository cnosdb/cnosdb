use trace::info;

pub fn backtrace() -> String {
    let res;
    #[cfg(feature = "backtrace")]
    {
        res = async_backtrace::taskdump_tree(true);
        info!("taskdump_tree: {}", res);
    }

    #[cfg(not(feature = "backtrace"))]
    {
        info!("not supported printing backtrace");
        res = "please export BACKTRACE=on".to_string();
    }
    res
}
