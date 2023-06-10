use std::cell::{Cell, Ref};
use std::time::SystemTime;

use fclones::PathAndMetadata;
use relm4::gtk::glib;
use relm4::gtk::glib::Object;
use relm4::gtk::prelude::ObjectExt;
use relm4::gtk::subclass::prelude::ObjectSubclassIsExt;

mod imp {
    use relm4::gtk::glib::{ParamSpec, Value};
    use relm4::gtk::prelude::{ParamSpecBuilderExt, ToValue};
    use relm4::gtk::subclass::prelude::*;
    use relm4::once_cell::sync::Lazy;
    use std::cell::RefCell;

    use super::*;

    #[derive(Default)]
    pub struct FileItem {
        pub inner: RefCell<Option<PathAndMetadata>>,
        pub selected: Cell<bool>,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for FileItem {
        const NAME: &'static str = "FileItem";
        type Type = super::FileItem;
    }

    impl ObjectImpl for FileItem {
        fn properties() -> &'static [ParamSpec] {
            static PROPERTIES: Lazy<Vec<ParamSpec>> = Lazy::new(|| {
                vec![
                    glib::ParamSpecString::builder("path").read_only().build(),
                    glib::ParamSpecUInt::builder("len").read_only().build(),
                    glib::ParamSpecBoolean::builder("selected").build(),
                ]
            });
            PROPERTIES.as_ref()
        }

        fn set_property(&self, _id: usize, value: &Value, pspec: &ParamSpec) {
            match pspec.name() {
                "selected" => {
                    let selected = value.get().unwrap();
                    self.selected.replace(selected);
                }
                _ => unimplemented!(),
            }
        }

        fn property(&self, _id: usize, pspec: &ParamSpec) -> Value {
            let inner = self.inner.borrow();
            let Some(inner) = inner.as_ref() else {
                return None::<String>.to_value();
            };

            match pspec.name() {
                "path" => inner.path.to_escaped_string().to_value(),
                "len" => inner.metadata.len().0.to_value(),
                "selected" => self.selected.get().to_value(),
                _ => unimplemented!(),
            }
        }
    }
}

glib::wrapper! {
    pub struct FileItem(ObjectSubclass<imp::FileItem>);
}

impl FileItem {
    pub fn new(f: fclones::PathAndMetadata) -> Self {
        let obj: Self = Object::new(&[]);
        let imp = obj.imp();
        imp.inner.replace(Some(f));
        obj
    }

    pub fn selected(&self) -> bool {
        self.imp().selected.get()
    }

    pub fn len(&self) -> fclones::FileLen {
        self.imp().inner.borrow().as_ref().unwrap().metadata.len()
    }

    pub fn modified_at(&self) -> Option<SystemTime> {
        self.imp()
            .inner
            .borrow()
            .as_ref()
            .unwrap()
            .metadata
            .modified()
            .ok()
    }

    pub fn path(&self) -> Ref<'_, fclones::Path> {
        let inner = self.imp().inner.borrow();
        Ref::map(inner, |p| &p.as_ref().unwrap().path)
    }

    pub fn as_ref(&self) -> FileItemRef<'_> {
        let inner = self.imp().inner.borrow();
        let path_and_metadata = Ref::map(inner, |p| p.as_ref().unwrap());
        FileItemRef {
            item: self,
            path_and_metadata,
        }
    }

    pub fn is_selected(&self) -> bool {
        self.imp().selected.get()
    }

    pub fn set_selected(&self, selected: bool) {
        self.imp().selected.set(selected);
        self.notify("selected")
    }

    pub fn to_path_and_metadata(&self) -> PathAndMetadata {
        self.imp().inner.borrow().as_ref().unwrap().clone()
    }
}

#[derive(Debug)]
pub struct FileItemRef<'a> {
    pub item: &'a FileItem,
    pub path_and_metadata: Ref<'a, PathAndMetadata>,
}

impl AsRef<PathAndMetadata> for FileItemRef<'_> {
    fn as_ref(&self) -> &PathAndMetadata {
        self.path_and_metadata.as_ref()
    }
}
