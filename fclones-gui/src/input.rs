use relm4::gtk;
use relm4::RelmWidgetExt;
use std::cell::Cell;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use adw::prelude::*;
use gtk::gio;
use gtk::glib;

use crate::app::AppMsg;
use crate::bytes_entry;
use crate::bytes_entry::*;
use crate::dir_chooser::choose_dir;

pub struct InputPageModel {
    // Input paths panel:
    current_dir: Rc<RefCell<Option<PathBuf>>>,
    input_paths: gio::ListStore,
    input_paths_selection: gtk::SingleSelection,
    // Search options:
    hidden_files: Rc<Cell<bool>>,
    respect_ignore: Rc<Cell<bool>>,
    follow_links: Rc<Cell<bool>>,
    match_links: Rc<Cell<bool>>,
    isolate: Rc<Cell<bool>>,
    min_rf: Rc<Cell<usize>>,
    min_size: Rc<Cell<u64>>,
    max_size: Rc<Cell<u64>>,
    // symbolic_links: Rc<Cell<bool>>,
    name_pattern: Rc<RefCell<String>>,
}

impl InputPageModel {
    pub fn new(paths: &[PathBuf]) -> InputPageModel {
        let input_paths = gio::ListStore::new(gtk::StringObject::static_type());
        for p in paths {
            input_paths.append(&gtk::StringObject::new(&p.to_string_lossy()))
        }
        let input_paths_selection = gtk::SingleSelection::new(Some(&input_paths));

        InputPageModel {
            current_dir: Rc::new(RefCell::new(None)),
            input_paths,
            input_paths_selection,
            hidden_files: Rc::new(Cell::new(false)),
            respect_ignore: Rc::new(Cell::new(false)),
            follow_links: Rc::new(Cell::new(false)),
            match_links: Rc::new(Cell::new(false)),
            isolate: Rc::new(Cell::new(false)),
            min_rf: Rc::new(Cell::new(2)),
            min_size: Rc::new(Cell::new(0)),
            max_size: Rc::new(Cell::new(u64::MAX)),
            // symbolic_links: Rc::new(Cell::new(false)),
            name_pattern: Rc::new(RefCell::new(String::from(""))),
        }
    }

    pub fn add_path(&self, path: PathBuf) {
        self.current_dir.replace(Some(path.clone()));
        self.input_paths
            .append(&gtk::StringObject::new(&path.to_string_lossy()));
        self.input_paths_selection
            .select_item(self.input_paths.n_items() - 1, true);
    }

    pub fn update_path(&self, position: u32, path: PathBuf) {
        self.current_dir.replace(Some(path.clone()));
        self.input_paths.remove(position);
        self.input_paths
            .insert(position, &gtk::StringObject::new(&path.to_string_lossy()));
        self.input_paths_selection.select_item(position, true);
    }

    pub fn remove_path(&self, position: u32) {
        self.input_paths.remove(position)
    }

    pub fn set_min_size(&self, value: u64) {
        self.min_size.set(value);
        if self.max_size.get() < value {
            self.max_size.set(value)
        }
    }

    pub fn set_max_size(&self, value: u64) {
        self.max_size.set(value);
        if self.min_size.get() > value {
            self.min_size.set(value)
        }
    }

    pub(crate) fn group_config(&self) -> fclones::config::GroupConfig {
        fclones::config::GroupConfig {
            paths: self.paths(),
            follow_links: self.follow_links.get(),
            hidden: self.hidden_files.get(),
            isolate: self.isolate.get(),
            match_links: self.match_links.get(),
            min_size: fclones::FileLen(self.min_size.get()),
            max_size: Some(fclones::FileLen(self.max_size.get())),
            name_patterns: self.name_patterns(),
            no_ignore: !self.respect_ignore.get(),
            regex: false,
            ..Default::default()
        }
    }

    fn paths(&self) -> Vec<fclones::Path> {
        self.input_paths
            .iter::<gtk::StringObject>()
            .unwrap()
            .map(|p| fclones::Path::from(p.unwrap().string().as_str()))
            .collect()
    }

    fn name_patterns(&self) -> Vec<String> {
        let name_pattern = self.name_pattern.borrow().clone();
        if name_pattern.is_empty() || name_pattern == "*" {
            vec![]
        } else {
            vec![name_pattern]
        }
    }
}

pub struct InputPageWidgets {
    pub root: gtk::Box,
    pub header: gtk::HeaderBar,
    window: gtk::ApplicationWindow,
    find_duplicates: gtk::Button,
    input_list: gtk::ListView,
    add_input_path: gtk::Button,
    hidden_files: gtk::Switch,
    respect_ignore: gtk::Switch,
    follow_links: gtk::Switch,
    match_links: gtk::Switch,
    isolate: gtk::Switch,
    min_rf: gtk::SpinButton,
    min_size: bytes_entry::BytesRow,
    max_size: bytes_entry::BytesRow,
    // symbolic_links: gtk::Switch,
    name_pattern: gtk::EditableLabel,
}

impl InputPageWidgets {
    pub fn new(window: &gtk::ApplicationWindow) -> Self {
        // ------------------------------------
        // Header
        // ------------------------------------
        let find_duplicates = gtk::Button::builder()
            .label("Find duplicates")
            .css_classes(vec!["suggested-action".to_string()])
            .build();
        let header = gtk::HeaderBar::new();
        header.pack_start(&find_duplicates);

        // ------------------------------------
        // Content
        // ------------------------------------

        let input_list = gtk::ListView::builder().hexpand(true).build();
        input_list.set_single_click_activate(true);
        input_list.add_css_class("rich-list");
        input_list.add_css_class("frame");

        let add_input_path = gtk::Button::builder()
            .child(
                &adw::ButtonContent::builder()
                    .label("Add")
                    .icon_name("list-add-symbolic")
                    .build(),
            )
            .has_frame(false)
            .build();
        let input_list_group = adw::PreferencesGroup::builder()
            .title("Directories to scan")
            .header_suffix(&add_input_path)
            .build();
        input_list_group.add(&input_list);

        let scan_preferences_group = adw::PreferencesGroup::builder()
            .title("Scan options")
            .build();
        let scan_preferences_list = gtk::ListBox::default();
        scan_preferences_list.add_css_class("boxed-list");
        scan_preferences_group.add(&scan_preferences_list);

        let hidden_files = add_bool_preference(
            "Hidden",
            "Searches files whose names start with a dot",
            &scan_preferences_list,
        );
        let no_ignore = add_bool_preference(
            "Respect ignore lists",
            "Respects .gitignore and .fdignore",
            &scan_preferences_list,
        );
        no_ignore.set_active(true);
        let follow_links = add_bool_preference(
            "Follow links",
            "Follows symbolic links to directories",
            &scan_preferences_list,
        );

        // TODO: Uncomment once symbolic links are handled by the dedupe panel safely
        // let symbolic_links = add_bool_preference(
        //     "Symbolic links",
        //     "Includes symbolic links to files",
        //     &scan_preferences_list,
        // );

        let min_size = BytesRow::new("Min size", "", "0 B", false);
        scan_preferences_list.append(min_size.row());

        let max_size = BytesRow::new("Max size", "", "Unlimited", true);
        scan_preferences_list.append(max_size.row());

        let name_pattern = add_text_preference(
            "Name pattern",
            "Includes only files whose name matches a glob pattern",
            &scan_preferences_list,
        );
        let match_preferences_group = adw::PreferencesGroup::builder()
            .title("Duplicate match options")
            .build();
        let match_preferences_list = gtk::ListBox::default();
        match_preferences_list.add_css_class("boxed-list");
        match_preferences_group.add(&match_preferences_list);

        let min_rf = add_spin_btn_preference(
            "Match count",
            "Minimum number of identical files to report",
            &match_preferences_list,
        );
        let adjustment = gtk::Adjustment::builder()
            .lower(2.0)
            .upper(100.0)
            .page_increment(1.0)
            .step_increment(1.0)
            .build();
        min_rf.configure(Some(&adjustment), 1.0, 0);
        min_rf.set_value(2.0);

        let match_links = add_bool_preference(
            "Match links",
            "Treats hard and symbolic links as ordinary files",
            &match_preferences_list,
        );
        let isolate = add_bool_preference(
            "Isolate",
            "Counts duplicates contained within a single input directory as one file",
            &match_preferences_list,
        );

        let preferences_columns = gtk::Box::new(gtk::Orientation::Horizontal, 20);
        preferences_columns.append(&scan_preferences_group);
        preferences_columns.append(&match_preferences_group);

        let root = gtk::Box::new(gtk::Orientation::Vertical, 20);
        root.set_margin_all(20);
        root.append(&input_list_group);
        root.append(&preferences_columns);

        InputPageWidgets {
            window: window.clone(),
            root,
            input_list,
            header,
            find_duplicates,
            add_input_path,
            hidden_files,
            respect_ignore: no_ignore,
            follow_links,
            // symbolic_links,
            name_pattern,
            match_links,
            isolate,
            min_rf,
            min_size,
            max_size,
        }
    }

    pub fn bind(&self, model: &InputPageModel, sender: relm4::Sender<AppMsg>) {
        self.find_duplicates
            .connect_clicked(glib::clone!(@strong sender => move |_|
                sender.send(AppMsg::FindDuplicates).unwrap()
            ));
        let factory = gtk::SignalListItemFactory::new();
        factory.connect_setup(glib::clone!(
            @strong sender,
            @strong self.input_list as list,
            @strong self.window as window => move |_, item|
        {
            let row = gtk::Box::builder()
                .orientation(gtk::Orientation::Horizontal)
                .build();
            let label = gtk::Label::builder()
                .halign(gtk::Align::Start)
                .hexpand(true)
                .ellipsize(gtk::pango::EllipsizeMode::Middle)
                .build();
            let remove_btn = gtk::Button::builder()
                .has_frame(false)
                .tooltip_text("Remove")
                .icon_name("list-remove-symbolic")
                .build();
            let edit_btn = gtk::Button::builder()
                .has_frame(false)
                .tooltip_text("Edit")
                .icon_name("edit-symbolic")
                .build();
            let row_buttons = gtk::Box::new(gtk::Orientation::Horizontal, 0);
            row_buttons.append(&edit_btn);
            row_buttons.append(&remove_btn);

            row.append(&label);
            row.append(&row_buttons);

            item.set_child(Some(&row));

            let pos_expression = item.property_expression("position");
            remove_btn.connect_clicked(glib::clone!(@strong sender, @strong pos_expression => move |_| {
                if let Some(pos) = pos_expression.evaluate(None::<&glib::Object>) {
                    let pos = pos.get::<u32>().unwrap();
                    sender.send(AppMsg::RemoveInputPath(pos)).unwrap()
                }
            }));
            edit_btn.connect_clicked(glib::clone!(
                @strong sender,
                @strong window,
                @strong list => move |_|
            {
                if let Some(pos) = pos_expression.evaluate(None::<&glib::Object>) {
                    let pos = pos.get::<u32>().unwrap();
                    edit_input_path(&window, &list, pos, &sender)
                }
            }));

            item.property_expression("item")
                .chain_property::<gtk::StringObject>("string")
                .bind(&label, "label", gtk::Widget::NONE);
        }));

        self.input_list.set_factory(Some(&factory));
        self.input_list
            .set_model(Some(&model.input_paths_selection));
        self.input_list.connect_activate(
            glib::clone!(@strong sender, @strong self.window as window =>
                    move |list, pos| edit_input_path(&window, list, pos, &sender)
            ),
        );

        let current_dir = model.current_dir.clone();
        self.add_input_path.connect_clicked(glib::clone!(
            @strong sender,
            @strong current_dir,
            @strong self.window as window => move |_|
        {
            let sender = sender.clone();
            choose_dir(
                &window,
                current_dir.borrow().clone(),
                false,
                move |paths| {
                    sender.send(AppMsg::AddInputPaths(paths)).unwrap();
                }
            );
        }));

        self.min_size
            .on_change(glib::clone!(@strong sender => move |value|
                sender.send(AppMsg::SetMinSize(value)).unwrap()
            ));
        self.max_size
            .on_change(glib::clone!(@strong sender => move |value|
                sender.send(AppMsg::SetMaxSize(value)).unwrap()
            ));

        bind_bool(&self.hidden_files, &model.hidden_files);
        bind_bool(&self.follow_links, &model.follow_links);
        // bind_bool(&self.symbolic_links, &model.symbolic_links);
        bind_bool(&self.isolate, &model.isolate);
        bind_bool(&self.match_links, &model.match_links);
        bind_bool(&self.respect_ignore, &model.respect_ignore);
        bind_string(&self.name_pattern, &model.name_pattern);
        bind_spin_btn(&self.min_rf, &model.min_rf);
    }

    pub fn update(&self, model: &InputPageModel) {
        self.min_size.set_value(model.min_size.get());
        self.max_size.set_value(model.max_size.get());
        self.find_duplicates
            .set_sensitive(model.input_paths.n_items() > 0);
    }
}

fn add_bool_preference(title: &str, subtitle: &str, list_box: &gtk::ListBox) -> gtk::Switch {
    let button = gtk::Switch::builder()
        .valign(gtk::Align::Center)
        .hexpand(false)
        .build();
    let row = adw::ActionRow::builder()
        .title(title)
        .subtitle(subtitle)
        .build();
    row.add_suffix(&button);
    row.set_activatable_widget(Some(&button));
    list_box.append(&row);
    button
}

fn add_spin_btn_preference(
    title: &str,
    subtitle: &str,
    list_box: &gtk::ListBox,
) -> gtk::SpinButton {
    let button = gtk::SpinButton::builder()
        .valign(gtk::Align::Center)
        .hexpand(false)
        .build();
    let row = adw::ActionRow::builder()
        .title(title)
        .subtitle(subtitle)
        .build();
    row.add_suffix(&button);
    list_box.append(&row);
    button
}

fn add_text_preference(title: &str, subtitle: &str, list_box: &gtk::ListBox) -> gtk::EditableLabel {
    let entry = gtk::EditableLabel::builder()
        .xalign(1.0)
        .valign(gtk::Align::Center)
        .halign(gtk::Align::End)
        .hexpand(false)
        .text("foo")
        .build();
    let edit_btn = gtk::Button::builder()
        .icon_name("edit-symbolic")
        .vexpand(false)
        .valign(gtk::Align::Center)
        .has_frame(false)
        .build();
    let row = adw::ActionRow::builder()
        .title(title)
        .subtitle(subtitle)
        .build();
    row.add_suffix(&entry);
    row.add_suffix(&edit_btn);
    row.set_activatable_widget(Some(&entry));
    row.connect_activated(glib::clone!(@strong entry => move |_| {
        entry.grab_focus();
        entry.start_editing();
    }));
    edit_btn.connect_clicked(glib::clone!(@strong entry => move |_| {
        entry.grab_focus();
        entry.start_editing();
    }));
    list_box.append(&row);
    entry
}

fn bind_bool(switch: &gtk::Switch, model: &Rc<Cell<bool>>) {
    let model = model.clone();
    switch.set_active(model.get());
    switch.connect_active_notify(move |switch| {
        model.set(switch.is_active());
    });
}

fn bind_string(entry: &gtk::EditableLabel, model: &Rc<RefCell<String>>) {
    let model = model.clone();
    entry.set_text(model.borrow().as_str());
    entry.connect_changed(move |entry| {
        *model.borrow_mut() = entry.text().to_string();
    });
}

fn bind_spin_btn(btn: &gtk::SpinButton, model: &Rc<Cell<usize>>) {
    let model = model.clone();
    btn.set_value(model.get() as f64);
    btn.connect_changed(move |btn| {
        model.set(btn.value() as usize);
    });
}

fn edit_input_path(
    window: &impl IsA<gtk::Window>,
    list: &gtk::ListView,
    position: u32,
    sender: &relm4::Sender<AppMsg>,
) {
    let current_item = list
        .model()
        .unwrap()
        .item(position)
        .unwrap()
        .downcast::<gtk::StringObject>()
        .unwrap();
    let current_path = PathBuf::from(current_item.string().as_str());
    let sender = sender.clone();
    choose_dir(window, Some(current_path), false, move |paths| {
        sender
            .send(AppMsg::UpdateInputPath(
                position,
                paths.into_iter().next().unwrap(),
            ))
            .unwrap()
    });
}
