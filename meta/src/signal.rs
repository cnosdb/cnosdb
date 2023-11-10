pub fn block_waiting_ctrl_c() {
    let (tx, rx) = std::sync::mpsc::channel();
    ctrlc::set_handler(move || tx.send(()).expect("cannot send signal on channel."))
        .expect("error setting Ctrl-C handler");
    println!("blocking waiting for Ctrl-C...");
    rx.recv().expect("could not receive from channel.");
    println!("\nreceived Ctrl-C, CnosDB meta is stoping...");
}
