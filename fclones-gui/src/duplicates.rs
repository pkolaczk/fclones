use adw::gio::{ListStore, SimpleAction};
use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::ops::Deref;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use adw::gtk::{ButtonsType, ColumnViewColumn, MessageDialog};
use fclones::{sort_by_priority, FileLen};
use fclones::{DedupeOp, FileSubGroup};
use fclones::{PartitionedFileGroup, Path};
use itertools::Itertools;
use relm4::gtk;

use adw::prelude::*;
use gtk::gio;
use gtk::glib;
use relm4::gtk::prelude::{ActionMapExt, ButtonExt};
use relm4::gtk::MessageType;

use crate::app::AppMsg;
use crate::dir_chooser::choose_dir;
use crate::file_group_item::FileGroupItem;
use crate::file_item::FileItem;

pub struct DuplicatesPageModel {
    files: gtk::TreeListModel,
    file_selection: gtk::SingleSelection,
    total_size: FileLen,
    total_count: usize,
    selected_size: Cell<FileLen>,
    selected_count: Rc<Cell<usize>>,
    selection_priority: Cell<fclones::Priority>,
    last_chosen_directory: Rc<RefCell<Option<PathBuf>>>,
    dedupe_op: DedupeOp,
}

impl DuplicatesPageModel {
    pub fn new() -> DuplicatesPageModel {
        let files = create_files_model();
        let file_selection = gtk::SingleSelection::new(Some(&files));
        DuplicatesPageModel {
            files,
            file_selection,
            total_count: 0,
            total_size: FileLen(0),
            selected_count: Rc::new(Cell::new(0)),
            selected_size: Cell::new(FileLen(0)),
            selection_priority: Cell::new(fclones::Priority::Top),
            last_chosen_directory: Rc::new(RefCell::new(None)),
            dedupe_op: DedupeOp::Remove,
        }
    }

    pub fn clear_files(&mut self) {
        self.root_store().remove_all();
        self.total_count = 0;
        self.total_size = FileLen(0);
        self.selected_count.set(0);
        self.selected_size.set(FileLen(0));
    }

    pub fn add_files(
        &mut self,
        start_id: usize,
        files: Vec<fclones::FileGroup<fclones::PathAndMetadata>>,
    ) {
        let count: usize = files.iter().map(fclones::FileGroup::file_count).sum();
        let size = files.iter().map(fclones::FileGroup::total_size).sum();
        let files = files
            .into_iter()
            .enumerate()
            .map(|(id, g)| FileGroupItem::new(start_id + id, g));
        self.total_count += count;
        self.total_size += size;
        self.root_store().extend(files);
    }

    pub(crate) fn remove_files(&mut self, position: u32, paths: &[Path]) {
        let paths = paths.iter().collect::<BTreeSet<_>>();
        let row = self.files.child_row(position).unwrap();
        row.item()
            .unwrap()
            .downcast::<FileGroupItem>()
            .unwrap()
            .remove_many(&paths);

        // If the row was expanded, let's remove items from the children model as well.
        // I guess there is a better way to do that automatically with GTK bindings,
        // but I haven't found it yet.
        if let Some(children) = row.children() {
            let children: ListStore = children.downcast().unwrap();
            for i in (0..children.n_items()).rev() {
                if let Some(child) = children.item(i) {
                    let file_item = child.downcast::<FileItem>().unwrap();
                    if paths.contains(file_item.path().as_ref()) {
                        self.total_count -= 1;
                        self.total_size -= file_item.len();
                        self.select_file(&file_item, false);
                        children.remove(i);
                    }
                }
            }
        }
    }

    /// Removes group of files at given position
    pub fn remove_group(&mut self, position: u32) {
        let Some(row) = self.files.child_row(position) else {
            return;
        };
        let group = row.item().unwrap().downcast::<FileGroupItem>().unwrap();
        for file in group.files().iter() {
            self.select_file(file, false);
        }
        self.total_count -= group.files().len();
        self.total_size -= group.total_size();
        self.root_store().remove(position);
    }

    /// Selects or unselects the file at given table row
    pub fn select_file_at(&self, position: u32, selected: bool) {
        let Some(row) = self.files.item(position) else {
            return;
        };

        let row: gtk::TreeListRow = row.downcast().unwrap();
        if let Ok(file) = row.item().unwrap().downcast::<FileItem>() {
            if file.selected() != selected {
                self.last_chosen_directory
                    .replace(Some(file.path().parent().unwrap().to_path_buf()));
                let group: FileGroupItem =
                    row.parent().unwrap().item().unwrap().downcast().unwrap();
                self.select_file(&file, selected);
                self.unselect_at_least_one(&group, &[&file]);
                group.set_selection_from_files();
            }
        } else if let Ok(group) = row.item().unwrap().downcast::<FileGroupItem>() {
            if group.selected() != selected {
                if selected {
                    self.select_group(&group);
                } else {
                    self.unselect_group(&group);
                }
            }
        }
    }

    pub fn select_files_in(&self, path: PathBuf) {
        self.last_chosen_directory.replace(Some(path.clone()));
        let path = Path::from(path);
        for row in self.files.iter::<gtk::TreeListRow>().unwrap() {
            if let Ok(group) = row.unwrap().item().unwrap().downcast::<FileGroupItem>() {
                let files = group.files();
                let files = self.sorted_by_priority(&files);
                let mut selected_files = vec![];
                for &file in files.iter().rev() {
                    if path.is_prefix_of(file.path().as_ref())
                        && selected_files.len() + 1 < files.len()
                    {
                        self.select_file(file, true);
                        selected_files.push(file);
                    }
                }
                self.unselect_at_least_one(&group, &selected_files);
                group.set_selection_from_files();
            }
        }
    }

    pub fn unselect_files_in(&self, path: PathBuf) {
        self.last_chosen_directory.replace(Some(path.clone()));
        let path = Path::from(path);
        for row in self.files.iter::<gtk::TreeListRow>().unwrap() {
            if let Ok(group) = row.unwrap().item().unwrap().downcast::<FileGroupItem>() {
                let files = group.files();
                for file in files.iter() {
                    if path.is_prefix_of(file.path().as_ref()) {
                        self.select_file(file, false);
                    }
                }
                group.set_selection_from_files();
            }
        }
    }

    /// Makes sure at least one file in the group is unselected
    fn unselect_at_least_one(&self, group: &FileGroupItem, ignore_files: &[&FileItem]) {
        let files = group.files();
        let files = self.sorted_by_priority(&files);
        let selected_count = files.iter().filter(|f| f.is_selected()).count();
        if selected_count == files.len() && !files.is_empty() {
            let to_unselect = files.iter().find(|&f| !ignore_files.contains(f));
            if let Some(to_unselect) = to_unselect {
                self.select_file(to_unselect, false);
            }
        }
    }

    pub fn set_selection_priority(&self, priority: fclones::Priority) {
        self.selection_priority.set(priority);

        // Re-select all already selected groups so they match the new priority
        for row in self.files.iter::<gtk::TreeListRow>().unwrap() {
            if let Ok(group) = row.unwrap().item().unwrap().downcast::<FileGroupItem>() {
                if group.selected() {
                    self.select_group(&group);
                }
            }
        }
    }

    pub fn select_all_files(&self) {
        for row in self.files.iter::<gtk::TreeListRow>().unwrap() {
            if let Ok(group) = row.unwrap().item().unwrap().downcast() {
                self.select_group(&group);
            }
        }
    }

    pub fn unselect_all_files(&self) {
        for row in self.files.iter::<gtk::TreeListRow>().unwrap() {
            if let Ok(group) = row.unwrap().item().unwrap().downcast::<FileGroupItem>() {
                self.unselect_group(&group);
            }
        }
    }

    fn unselect_group(&self, group: &FileGroupItem) {
        group.set_selected(false);
        for file in group.files().iter() {
            self.select_file(file, false);
        }
        group.set_selection_from_files();
    }

    fn select_group(&self, group: &FileGroupItem) {
        group.set_selected(true);
        let files = group.files();
        let files = self.sorted_by_priority(&files);
        let mut first = true;
        for sg in files.iter() {
            self.select_file(sg, !first);
            first = false;
        }
    }

    fn sorted_by_priority<'a>(&self, files: &'a [FileItem]) -> Vec<&'a FileItem> {
        let mut files = files
            .iter()
            .map(FileItem::as_ref)
            .map(FileSubGroup::single)
            .collect_vec();
        sort_by_priority(&mut files, &self.selection_priority.get());
        files
            .into_iter()
            .map(|f| f.files.into_iter().next().unwrap().item)
            .collect_vec()
    }

    fn select_file(&self, file: &FileItem, selected: bool) {
        match (file.is_selected(), selected) {
            (false, true) => {
                self.selected_count.set(self.selected_count.get() + 1);
                self.selected_size
                    .set(self.selected_size.get() + file.len());
                file.set_selected(selected);
            }
            (true, false) => {
                self.selected_count.set(self.selected_count.get() - 1);
                self.selected_size
                    .set(self.selected_size.get() - file.len());
                file.set_selected(selected);
            }
            (_, _) => {}
        }
    }

    /// Returns information on selected and not-selected files in each group.
    /// Used for performing a deduplicate action on selected files.
    pub fn partitioned_groups(&self) -> Vec<PartitionedFileGroup> {
        let mut groups = Vec::new();
        for row in self.files.iter::<gtk::TreeListRow>().unwrap() {
            if let Ok(group) = row.unwrap().item().unwrap().downcast::<FileGroupItem>() {
                groups.push(group.to_partitioned_file_group());
            }
        }
        groups
    }

    fn root_store(&self) -> gio::ListStore {
        self.files.model().downcast().unwrap()
    }

    pub fn dedupe_op(&self) -> &DedupeOp {
        &self.dedupe_op
    }

    pub fn set_dedupe_op(&mut self, op: DedupeOp) {
        self.dedupe_op = op;
    }
}

fn create_files_model() -> gtk::TreeListModel {
    let files = gio::ListStore::new(FileGroupItem::static_type());
    gtk::TreeListModel::new(&files, false, true, |parent| {
        if let Some(group) = parent.downcast_ref::<FileGroupItem>() {
            let store = gio::ListStore::new(FileItem::static_type());
            for file in group.files().iter() {
                store.append(file);
            }
            Some(store.upcast())
        } else {
            None
        }
    })
}

pub struct DuplicatesPageWidgets {
    pub root: gtk::Box,
    pub header: gtk::HeaderBar,
    window: gtk::ApplicationWindow,
    files: gtk::ColumnView,
    name_col: gtk::ColumnViewColumn,
    modified_col: gtk::ColumnViewColumn,
    size_col: gtk::ColumnViewColumn,
    select_col: gtk::ColumnViewColumn,
    file_stats: gtk::Statusbar,
    selection_stats: gtk::Statusbar,
    prev_page_btn: gtk::Button,
    deduplicate_btn: adw::SplitButton,
    selection_priority: gtk::DropDown,
}

const SELECTION_PRIORITIES: [(&str, fclones::Priority); 10] = [
    ("Top", fclones::Priority::Top),
    ("Bottom", fclones::Priority::Bottom),
    ("Newest", fclones::Priority::Newest),
    ("Oldest", fclones::Priority::Oldest),
    (
        "Most recently modified",
        fclones::Priority::MostRecentlyModified,
    ),
    (
        "Least recently modified",
        fclones::Priority::LeastRecentlyModified,
    ),
    (
        "Most recently accessed",
        fclones::Priority::MostRecentlyAccessed,
    ),
    (
        "Least recently accessed",
        fclones::Priority::LeastRecentlyAccessed,
    ),
    ("Most nested", fclones::Priority::MostNested),
    ("Least nested", fclones::Priority::LeastNested),
];

// Menu label, button label, tooltip, action
const DEDUPE_MENU_ITEMS: [(&str, &str, &str, &str); 5] = [
    (
        "Remove",
        "Remove",
        "Remove selected duplicates",
        "win.remove",
    ),
    (
        "Move to...",
        "Move",
        "Move selected duplicates to",
        "win.move",
    ),
    (
        "Link",
        "Link",
        "Replace selected duplicates with hard links",
        "win.hard_link",
    ),
    (
        "Symbolic link",
        "Symbolic link",
        "Replace selected duplicates with symbolic links",
        "win.symbolic_link",
    ),
    (
        "Deduplicate",
        "Deduplicate",
        "Make duplicates share data using reflink",
        "win.dedupe",
    ),
];

impl DuplicatesPageWidgets {
    pub fn new(window: &gtk::ApplicationWindow) -> DuplicatesPageWidgets {
        // --------------------
        // Header bar
        // --------------------
        let prev_page_btn = gtk::Button::builder()
            .icon_name("go-previous-symbolic")
            .tooltip_text("Back to input page")
            .build();

        let deduplicate_btn_menu = gio::Menu::new();
        for (label, _, _, action) in DEDUPE_MENU_ITEMS {
            deduplicate_btn_menu.append(Some(label), Some(action));
        }

        let deduplicate_btn = adw::SplitButton::builder()
            .label(DEDUPE_MENU_ITEMS[0].1)
            .css_classes(vec!["destructive-action".to_string()])
            .menu_model(&deduplicate_btn_menu)
            .tooltip_text(DEDUPE_MENU_ITEMS[0].2)
            .build();

        let selection_model = gtk::StringList::new(&SELECTION_PRIORITIES.map(|p| p.0));
        let selection_priority = gtk::DropDown::builder()
            .model(&selection_model)
            .tooltip_text("Selection priority")
            .build();

        let select_btn_menu = gio::Menu::new();
        select_btn_menu.append(Some("Select all duplicates"), Some("win.select"));
        select_btn_menu.append(
            Some("Select all duplicates in directory..."),
            Some("win.select_in_directory"),
        );
        select_btn_menu.append(
            Some("Select all duplicates matching pattern..."),
            Some("win.select_by_name"),
        );

        let select_btn = gtk::MenuButton::builder()
            .icon_name("checkbox-checked-symbolic")
            .tooltip_text("Select files")
            .menu_model(&select_btn_menu)
            .build();

        let unselect_btn_menu = gio::Menu::new();
        unselect_btn_menu.append(Some("Unselect all"), Some("win.unselect"));
        unselect_btn_menu.append(
            Some("Unselect all in directory..."),
            Some("win.unselect_in_directory"),
        );
        unselect_btn_menu.append(
            Some("Unselect all matching pattern..."),
            Some("win.unselect_by_name"),
        );

        let unselect_btn = gtk::MenuButton::builder()
            .icon_name("checkbox-symbolic")
            .menu_model(&unselect_btn_menu)
            .tooltip_text("Unselect files")
            .build();

        let select_box = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        select_box.add_css_class("linked");
        select_box.append(&selection_priority);
        select_box.append(&select_btn);
        select_box.append(&unselect_btn);

        let header = gtk::HeaderBar::default();
        header.pack_start(&prev_page_btn);
        header.pack_start(&deduplicate_btn);
        header.pack_end(&select_box);

        // --------------------
        // Window content
        // --------------------
        let name_column = ColumnViewColumn::builder()
            .title("Name / Hash")
            .expand(true)
            .build();
        let size_column = ColumnViewColumn::builder().title("Size").build();
        let modified_column = ColumnViewColumn::builder().title("Modified").build();
        let select_column = ColumnViewColumn::default();
        select_column.set_fixed_width(50);

        let duplicates_view = gtk::ColumnView::builder()
            .vexpand(true)
            .hexpand(true)
            .single_click_activate(true)
            .build();

        duplicates_view.add_css_class("data-table");
        duplicates_view.append_column(&name_column);
        duplicates_view.append_column(&modified_column);
        duplicates_view.append_column(&size_column);
        duplicates_view.append_column(&select_column);

        column_header(&duplicates_view, 1).set_halign(gtk::Align::End);
        column_header(&duplicates_view, 2).set_halign(gtk::Align::End);

        let duplicates_scroll = gtk::ScrolledWindow::default();
        duplicates_scroll.set_hscrollbar_policy(gtk::PolicyType::Never);
        duplicates_scroll.set_child(Some(&duplicates_view));

        let status_bar = gtk::Statusbar::new();
        status_bar.set_hexpand(true);

        let file_stats = gtk::Statusbar::new();
        let selection_stats = gtk::Statusbar::new();

        let status_box = gtk::Box::new(gtk::Orientation::Horizontal, 0);
        status_box.set_hexpand(true);
        status_box.append(&status_bar);
        status_box.append(&file_stats);
        status_box.append(&gtk::Separator::default());
        status_box.append(&selection_stats);

        let root = gtk::Box::builder()
            .orientation(gtk::Orientation::Vertical)
            .build();
        root.append(&duplicates_scroll);
        root.append(&gtk::Separator::default());
        root.append(&status_box);

        DuplicatesPageWidgets {
            window: window.clone(),
            root,
            files: duplicates_view,
            name_col: name_column,
            size_col: size_column,
            select_col: select_column,
            header,
            prev_page_btn,
            deduplicate_btn,
            file_stats,
            selection_stats,
            modified_col: modified_column,
            selection_priority,
        }
    }

    pub fn bind(&self, model: &DuplicatesPageModel, sender: relm4::Sender<AppMsg>) {
        self.name_col.set_factory(Some(&name_factory()));
        self.modified_col
            .set_factory(Some(&modification_time_factory()));
        self.size_col.set_factory(Some(&size_factory()));
        self.select_col
            .set_factory(Some(&checkbox_factory(sender.clone())));

        self.files.set_model(Some(&model.file_selection));
        self.files
            .connect_activate(glib::clone!(@strong sender => move |list_view, position| {
                let row = list_view
                    .model()
                    .unwrap()
                    .item(position)
                    .unwrap()
                    .downcast::<gtk::TreeListRow>()
                    .unwrap();
                row.set_expanded(!row.is_expanded());
                if let Ok(file) = row.item().unwrap().downcast::<FileItem>() {
                    sender.send(AppMsg::ToggleFileSelection(
                        row.position(),
                        !file.selected(),
                    )).unwrap();
                };
            }));

        self.prev_page_btn
            .connect_clicked(glib::clone!(@strong sender => move |_|
                sender.send(AppMsg::ActivateInputPage).unwrap_or_default()));

        self.bind_select_action("unselect", AppMsg::UnselectAllFiles, &sender);
        self.bind_select_action("select", AppMsg::SelectAllFiles, &sender);
        self.bind_select_files_in_directory(model, &sender);
        self.bind_unselect_files_in_directory(model, &sender);

        self.selection_priority
            .connect_selected_notify(glib::clone!(@strong sender => move |dd| {
                let priority = SELECTION_PRIORITIES[dd.selected() as usize].1;
                sender.send(AppMsg::SelectionPriorityChanged(priority)).unwrap_or_default();
            }));

        self.bind_dedupe_op_select_action(
            "remove",
            0,
            AppMsg::SetDedupeOp(DedupeOp::Remove),
            &sender,
        );
        self.bind_dedupe_op_select_move_action(1, model, &sender);
        self.bind_dedupe_op_select_action(
            "hard_link",
            2,
            AppMsg::SetDedupeOp(DedupeOp::HardLink),
            &sender,
        );
        self.bind_dedupe_op_select_action(
            "symbolic_link",
            3,
            AppMsg::SetDedupeOp(DedupeOp::SymbolicLink),
            &sender,
        );
        self.bind_dedupe_op_select_action(
            "dedupe",
            4,
            AppMsg::SetDedupeOp(DedupeOp::RefLink),
            &sender,
        );

        let selected_count = model.selected_count.clone();
        let window = &self.window;
        self.deduplicate_btn.connect_clicked(
            glib::clone!(@strong sender, @weak window => move |_| {
                if selected_count.get() == 0 {
                    MessageDialog::builder()
                        .text("No files selected")
                        .secondary_text("Please select some duplicates first")
                        .decorated(true)
                        .modal(true)
                        .message_type(MessageType::Error)
                        .transient_for(&window)
                        .buttons(ButtonsType::Ok)
                        .build()
                        .run_async(|dlg, _| { dlg.destroy() });
                } else {
                    sender.send(AppMsg::Deduplicate).unwrap_or_default();
                }
            }),
        );
    }

    fn bind_select_action(&self, action_name: &str, msg: AppMsg, sender: &relm4::Sender<AppMsg>) {
        let select_action = SimpleAction::new(action_name, None);
        let sender = sender.clone();
        select_action.connect_activate(move |_, _| sender.send(msg.clone()).unwrap());
        self.window.add_action(&select_action);
    }

    fn bind_dedupe_op_select_action(
        &self,
        action_name: &str,
        menu_index: usize,
        msg: AppMsg,
        sender: &relm4::Sender<AppMsg>,
    ) {
        let dedupe_action = SimpleAction::new(action_name, None);
        let sender = sender.clone();
        let deduplicate_btn = self.deduplicate_btn.clone();
        dedupe_action.connect_activate(glib::clone!(@weak deduplicate_btn => move |_, _| {
            deduplicate_btn.set_label(DEDUPE_MENU_ITEMS[menu_index].1);
            deduplicate_btn.set_tooltip_text(Some(DEDUPE_MENU_ITEMS[menu_index].2));
            sender.send(msg.clone()).unwrap()
        }));
        self.window.add_action(&dedupe_action);
    }

    fn bind_dedupe_op_select_move_action(
        &self,
        menu_index: usize,
        model: &DuplicatesPageModel,
        sender: &relm4::Sender<AppMsg>,
    ) {
        let dedupe_action = SimpleAction::new("move", None);
        let sender = sender.clone();
        let deduplicate_btn = self.deduplicate_btn.clone();
        let window = self.window.clone();
        let last_selection = model.last_chosen_directory.clone();

        dedupe_action.connect_activate(glib::clone!(@weak deduplicate_btn => move |_, _| {
            let sender = sender.clone();
            let start_dir = last_selection
                .deref()
                .borrow()
                .as_ref()
                .map(|f| f.to_path_buf());
            choose_dir(&window, start_dir, true, move |paths| {
                let path = paths.into_iter().next().unwrap();
                let path = Arc::new(fclones::Path::from(path));
                deduplicate_btn.set_label(
                    format!("Move to {}", path.file_name().unwrap_or_default().to_string_lossy())
                        .as_str());
                deduplicate_btn.set_tooltip_text(Some(
                    format!("{} {}", DEDUPE_MENU_ITEMS[menu_index].2, path.display()).as_str()));
                let msg = AppMsg::SetDedupeOp(DedupeOp::Move(path));
                sender.send(msg).unwrap();
            });
        }));
        self.window.add_action(&dedupe_action);
    }

    fn bind_select_files_in_directory(
        &self,
        model: &DuplicatesPageModel,
        sender: &relm4::Sender<AppMsg>,
    ) {
        let select_action = SimpleAction::new("select_in_directory", None);
        let sender = sender.clone();
        let window = self.window.clone();
        let last_selection = model.last_chosen_directory.clone();
        select_action.connect_activate(move |_, _| {
            let sender = sender.clone();
            let start_dir = last_selection
                .deref()
                .borrow()
                .as_ref()
                .map(|f| f.to_path_buf());
            choose_dir(&window, start_dir, false, move |paths| {
                sender
                    .send(AppMsg::SelectFilesInDirectory(
                        paths.into_iter().next().unwrap(),
                    ))
                    .unwrap()
            });
        });
        self.window.add_action(&select_action);
    }

    fn bind_unselect_files_in_directory(
        &self,
        model: &DuplicatesPageModel,
        sender: &relm4::Sender<AppMsg>,
    ) {
        let select_action = SimpleAction::new("unselect_in_directory", None);
        let sender = sender.clone();
        let window = self.window.clone();
        let last_selection = model.last_chosen_directory.clone();
        select_action.connect_activate(move |_, _| {
            let sender = sender.clone();
            let start_dir = last_selection
                .deref()
                .borrow()
                .as_ref()
                .map(|f| f.to_path_buf());
            choose_dir(&window, start_dir, false, move |paths| {
                sender
                    .send(AppMsg::UnselectFilesInDirectory(
                        paths.into_iter().next().unwrap(),
                    ))
                    .unwrap()
            });
        });
        self.window.add_action(&select_action);
    }

    pub fn update(&self, model: &DuplicatesPageModel) {
        let file_stats = format!(
            "Total {} in {} {}",
            model.total_size,
            model.total_count,
            if model.total_count == 1 {
                "file"
            } else {
                "files"
            }
        );
        let selection_stats = format!(
            "Selected {} in {} {}",
            model.selected_size.get(),
            model.selected_count.get(),
            if model.selected_count.get() == 1 {
                "file"
            } else {
                "files"
            }
        );
        let id = self.file_stats.context_id("stats");
        self.file_stats.remove_all(id);
        self.file_stats.push(id, file_stats.as_str());

        let id = self.selection_stats.context_id("stats");
        self.selection_stats.remove_all(id);
        self.selection_stats.push(id, selection_stats.as_str());
    }
}

fn column_header(view: &gtk::ColumnView, n: usize) -> gtk::Box {
    // A hack to get N-th ColumnView header. A pity Gtk4 doesn't support this directly.
    let mut column = view.first_child().unwrap().first_child().unwrap();
    for _ in 0..n {
        column = column.next_sibling().unwrap();
    }
    column.first_child().unwrap().downcast().unwrap()
}

fn name_factory() -> gtk::SignalListItemFactory {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(move |_, list_item| {
        // We must wrap the TreeExpander in a Box, due to a weird scrolling bug
        // in GTK+ 4.6.6. If the TreeExpander is not wrapped in a Box, sometimes clicking a row
        // will cause the view to be scrolled up so that the row appears as the first row.
        let expander = gtk::TreeExpander::new();
        let b = gtk::Box::default();
        b.append(&expander);
        list_item.set_child(Some(&b));
    });
    factory.connect_bind(move |_, list_item| {
        let row = list_item
            .item()
            .unwrap()
            .downcast::<gtk::TreeListRow>()
            .unwrap();
        let expander = list_item
            .child()
            .unwrap()
            .first_child()
            .unwrap()
            .downcast::<gtk::TreeExpander>()
            .unwrap();
        expander.set_list_row(Some(&row));
        if let Ok(file) = row.item().unwrap().downcast::<FileItem>() {
            let label = gtk::Label::builder()
                .ellipsize(gtk::pango::EllipsizeMode::Middle)
                .label(file.path().to_escaped_string().as_str())
                .build();
            expander.set_child(Some(&label));
        };

        if let Ok(group) = row.item().unwrap().downcast::<FileGroupItem>() {
            let group_name = format!("Group {}", group.id() + 1);
            let label = gtk::Label::new(Some(group_name.as_str()));
            label.add_css_class("heading");
            expander.set_child(Some(&label));
        };
    });
    factory
}

fn size_factory() -> gtk::SignalListItemFactory {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(move |_, list_item| {
        let label = gtk::Label::new(None);
        label.set_xalign(1.0);
        label.add_css_class("light");
        label.add_css_class("numeric");
        label.set_margin_start(5);
        list_item.set_child(Some(&label));
    });
    factory.connect_bind(move |_, list_item| {
        let row = list_item
            .item()
            .unwrap()
            .downcast::<gtk::TreeListRow>()
            .unwrap();
        let label = list_item.child().unwrap().downcast::<gtk::Label>().unwrap();
        if let Ok(file) = row.item().unwrap().downcast::<FileItem>() {
            label.set_label(file.len().to_string().as_str());
        }
        if let Ok(group) = row.item().unwrap().downcast::<FileGroupItem>() {
            label.set_label(group.total_size().to_string().as_str());
        }
    });
    factory
}

fn modification_time_factory() -> gtk::SignalListItemFactory {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(move |_, list_item| {
        let label = gtk::Label::new(None);
        label.set_xalign(1.0);
        label.add_css_class("light");
        label.add_css_class("numeric");
        label.set_margin_start(5);
        list_item.set_child(Some(&label));
    });
    factory.connect_bind(move |_, list_item| {
        let row = list_item
            .item()
            .unwrap()
            .downcast::<gtk::TreeListRow>()
            .unwrap();
        let label = list_item.child().unwrap().downcast::<gtk::Label>().unwrap();
        label.set_label("");
        if let Ok(file) = row.item().unwrap().downcast::<FileItem>() {
            if let Some(modified_at) = file.modified_at() {
                let datetime: chrono::DateTime<chrono::Local> = modified_at.into();
                label.set_label(format!("{}", datetime.format("%Y-%m-%d")).as_str());
            }
        }
    });
    factory
}

fn checkbox_factory(sender: relm4::Sender<AppMsg>) -> gtk::SignalListItemFactory {
    let factory = gtk::SignalListItemFactory::new();
    factory.connect_setup(move |_, item| {
        let sender = sender.clone();
        let check = gtk::CheckButton::new();
        check.set_halign(gtk::Align::Center);
        check.set_margin_start(5);

        let pos_expression = item.property_expression("position");
        check.connect_toggled(move |check| {
            if let Some(pos) = pos_expression.evaluate(None::<&glib::Object>) {
                let pos = pos.get::<u32>().unwrap();
                sender
                    .send(AppMsg::ToggleFileSelection(pos, check.is_active()))
                    .unwrap();
            }
        });

        // We must wrap the CheckBox in a Box, due to the same issue as with TreeExpander.
        // There is a weird scrolling bug in GTK+ 4.6.6. If the CheckButton is not wrapped in a Box,
        // sometimes clicking a row will cause the view to be scrolled up so that the row appears
        // as the first row.
        let b = gtk::Box::default();
        b.append(&check);
        item.set_child(Some(&b));

        item.property_expression("item")
            .chain_property::<gtk::TreeListRow>("item")
            .chain_property::<FileItem>("selected")
            .bind(&check, "active", gtk::Widget::NONE);

        item.property_expression("item")
            .chain_property::<gtk::TreeListRow>("item")
            .chain_property::<FileGroupItem>("selected")
            .bind(&check, "active", gtk::Widget::NONE);

        item.property_expression("item")
            .chain_property::<gtk::TreeListRow>("item")
            .chain_property::<FileGroupItem>("inconsistent")
            .bind(&check, "inconsistent", gtk::Widget::NONE);
    });
    factory
}
