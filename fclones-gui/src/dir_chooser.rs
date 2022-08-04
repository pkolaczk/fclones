use std::path::PathBuf;

use adw::prelude::*;
use relm4::gtk;
use relm4::gtk::gio;
use relm4::gtk::glib;

/// Displays a FileChooserDialog and lets the user select input directories.
/// If the user accepts the selection, passes the selected path to the given `accept_fn`.
pub fn choose_dir(
    window: &impl IsA<gtk::Window>,
    current_file: Option<PathBuf>,
    create_folders: bool,
    accept_fn: impl Fn(Vec<PathBuf>) + 'static,
) {
    let file_chooser = gtk::FileChooserNative::new(
        Some("Select directory"),
        Some(&window.clone()),
        gtk::FileChooserAction::SelectFolder,
        Some("Select"),
        Some("Cancel"),
    );

    file_chooser.set_modal(true);
    file_chooser.set_create_folders(create_folders);
    file_chooser.set_transient_for(Some(window));

    if let Some(file) = current_file {
        let file = gio::File::for_path(file);
        if let Err(err) = file_chooser.set_file(&file) {
            eprintln!("Failed to set FileChooser file: {}", err);
        }
    }

    file_chooser.connect_response(glib::clone!(@strong file_chooser => move |_, result| {
        if result == gtk::ResponseType::Accept {
            let mut paths = Vec::new();
            for f in file_chooser.files().into_iter().flatten() {
                if let Ok(f) = f.downcast::<gio::File>() {
                    if let Some(path) = f.path() {
                        paths.push(path);
                    }
                }
            }
            accept_fn(paths);
        }
        file_chooser.destroy();
    }));

    file_chooser.show();
}
