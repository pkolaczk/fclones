use adw::glib;
use adw::glib::ObjectExt;
use adw::subclass::prelude::*;
use std::cell::{Ref, RefCell};
use std::collections::BTreeSet;
use std::ops::Deref;

use crate::file_group_item::imp::Selection;
use fclones::{FileHash, FileLen, PartitionedFileGroup};

use crate::file_item::FileItem;

mod imp {
    use std::cell::Cell;

    use adw::prelude::*;
    use relm4::once_cell::sync::Lazy;

    use super::*;

    use crate::file_item::FileItem;

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum Selection {
        #[default]
        None,
        Inconsistent,
        Selected,
    }

    #[derive(Default)]
    pub struct FileGroupItem {
        pub id: Cell<usize>,
        pub file_hash: RefCell<FileHash>,
        pub file_len: RefCell<FileLen>,
        pub files: RefCell<Vec<FileItem>>,
        pub selected: Cell<Selection>,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for FileGroupItem {
        const NAME: &'static str = "FileGroupItem";
        type Type = super::FileGroupItem;
    }

    impl ObjectImpl for FileGroupItem {
        fn properties() -> &'static [glib::ParamSpec] {
            static PROPERTIES: Lazy<Vec<glib::ParamSpec>> = Lazy::new(|| {
                vec![
                    glib::ParamSpecBoolean::builder("selected")
                        .read_only()
                        .build(),
                    glib::ParamSpecBoolean::builder("inconsistent")
                        .read_only()
                        .build(),
                ]
            });
            PROPERTIES.as_ref()
        }

        fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
            match pspec.name() {
                "selected" => (self.selected.get() == Selection::Selected).to_value(),
                "inconsistent" => (self.selected.get() == Selection::Inconsistent).to_value(),
                _ => unimplemented!(),
            }
        }
    }
}

glib::wrapper! {
    pub struct FileGroupItem(ObjectSubclass<imp::FileGroupItem>);
}

impl FileGroupItem {
    pub fn new(id: usize, fg: fclones::FileGroup<fclones::PathAndMetadata>) -> FileGroupItem {
        let obj: Self = glib::Object::new(&[]);
        let imp = obj.imp();
        imp.id.set(id);
        imp.file_hash.replace(fg.file_hash);
        imp.file_len.replace(fg.file_len);
        imp.files
            .replace(fg.files.into_iter().map(FileItem::new).collect());
        obj
    }

    pub fn id(&self) -> usize {
        self.imp().id.get()
    }

    pub fn file_hash(&self) -> Ref<'_, FileHash> {
        self.imp().file_hash.borrow()
    }

    pub fn file_len(&self) -> FileLen {
        *self.imp().file_len.borrow()
    }

    pub fn files(&self) -> Ref<'_, Vec<FileItem>> {
        self.imp().files.borrow()
    }

    pub fn total_size(&self) -> FileLen {
        self.imp().files.borrow().iter().map(FileItem::len).sum()
    }

    pub fn selected(&self) -> bool {
        self.property("selected")
    }

    pub fn set_selected(&self, selected: bool) {
        if selected {
            self.imp().selected.set(Selection::Selected)
        } else {
            self.imp().selected.set(Selection::None)
        }
        self.notify("selected");
        self.notify("inconsistent");
    }

    /// Updates selection based on child items selection
    pub fn set_selection_from_files(&self) {
        let files = self.files();
        let checked_count = files.iter().filter(|f| f.is_selected()).count();
        let selection = if checked_count > 0 && files.len() - checked_count <= 1 {
            Selection::Selected
        } else if checked_count > 0 && files.len() - checked_count > 1 {
            Selection::Inconsistent
        } else {
            Selection::None
        };
        if selection != self.imp().selected.get() {
            self.imp().selected.set(selection);
            self.notify("selected");
            self.notify("inconsistent");
        }
    }

    pub fn remove_many(&self, paths: &BTreeSet<&fclones::Path>) {
        let mut self_files = self.imp().files.borrow_mut();
        self_files.retain(|f| !paths.contains(f.path().as_ref()));
        drop(self_files);
        self.set_selection_from_files();
    }

    pub fn to_partitioned_file_group(&self) -> PartitionedFileGroup {
        let mut to_keep = Vec::new();
        let mut to_drop = Vec::new();
        for file_item in self.files().deref() {
            let metadata = file_item.to_path_and_metadata();
            if file_item.is_selected() {
                to_drop.push(metadata)
            } else {
                to_keep.push(metadata)
            }
        }
        PartitionedFileGroup { to_keep, to_drop }
    }
}
