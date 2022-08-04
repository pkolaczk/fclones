use std::env;

use relm4::RelmApp;

mod app;
mod bytes_entry;
mod dedupe_worker;
mod dir_chooser;
mod duplicates;
mod file_group_item;
mod file_item;
mod group_worker;
mod input;
mod progress;

fn main() {
    adw::init().unwrap();
    let relm = RelmApp::new("pkolaczk.fclones");
    relm.run::<app::AppModel>(env::current_dir().iter().cloned().collect());
}
