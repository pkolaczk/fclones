use adw::gtk::{MessageDialog, MessageType};
use adw::prelude::{DialogExt, WidgetExt};
use std::convert::identity;
use std::path::PathBuf;

use gtk::{CssProvider, StyleContext};
use relm4::gtk::prelude::GtkWindowExt;
use relm4::gtk::{gdk, ButtonsType};
use relm4::Component;
use relm4::ComponentController;
use relm4::WorkerController;
use relm4::{gtk, ComponentParts, ComponentSender, Controller, SimpleComponent};

use fclones::DedupeOp;

use crate::dedupe_worker::{DedupeWorker, DedupeWorkerMsg};
use crate::duplicates::{DuplicatesPageModel, DuplicatesPageWidgets};
use crate::group_worker::GroupWorker;
use crate::group_worker::GroupWorkerMsg;
use crate::input::{InputPageModel, InputPageWidgets};
use crate::progress::{ProgressDialog, ProgressMsg};

#[derive(Debug, Clone)]
pub enum AppMsg {
    ActivateInputPage,
    AddInputPaths(Vec<PathBuf>),
    UpdateInputPath(u32, PathBuf),
    RemoveInputPath(u32),
    SetMinSize(u64),
    SetMaxSize(u64),
    FindDuplicates,
    AbortSearch,
    Deduplicate,
    SetDedupeOp(DedupeOp),
    ClearFiles,
    AddFiles(usize, Vec<fclones::FileGroup<fclones::PathAndMetadata>>),
    RemoveFiles(u32, Vec<fclones::Path>),
    RemoveGroup(u32),
    NoDuplicatesFound,
    Progress(ProgressMsg),
    ToggleFileSelection(u32, bool),
    SelectionPriorityChanged(fclones::Priority),
    SelectAllFiles,
    SelectFilesInDirectory(PathBuf),
    UnselectAllFiles,
    UnselectFilesInDirectory(PathBuf),
}

pub enum AppPage {
    Input,
    Duplicates,
    NoDuplicatesMsg,
}

pub struct AppModel {
    selected_page: AppPage,
    input: InputPageModel,
    duplicates: DuplicatesPageModel,
    group_worker: WorkerController<GroupWorker>,
    dedupe_worker: WorkerController<DedupeWorker>,
    progress: Controller<ProgressDialog>,
}

pub struct AppWidgets {
    window: gtk::ApplicationWindow,
    container: gtk::Stack,
    input_page: InputPageWidgets,
    duplicates_page: DuplicatesPageWidgets,
    no_dupes_msg_dlg: MessageDialog,
}

impl SimpleComponent for AppModel {
    type Input = AppMsg;
    type Output = ();
    type Init = Vec<PathBuf>;
    type Root = gtk::ApplicationWindow;
    type Widgets = AppWidgets;

    fn init_root() -> Self::Root {
        load_css();
        gtk::ApplicationWindow::builder()
            .title("FClones")
            .default_width(1200)
            .default_height(200)
            .build()
    }

    fn init(
        paths: Self::Init,
        root: &Self::Root,
        sender: ComponentSender<Self>,
    ) -> ComponentParts<Self> {
        let input_page = InputPageWidgets::new(root);
        let duplicates_page = DuplicatesPageWidgets::new(root);

        let container = gtk::Stack::new();
        container.add_child(&input_page.root);
        container.add_child(&duplicates_page.root);

        let model = AppModel {
            selected_page: AppPage::Input,
            input: InputPageModel::new(&paths),
            duplicates: DuplicatesPageModel::new(),
            group_worker: GroupWorker::builder()
                .detach_worker(())
                .forward(sender.input_sender(), identity),
            dedupe_worker: DedupeWorker::builder()
                .detach_worker(())
                .forward(sender.input_sender(), identity),
            progress: ProgressDialog::builder()
                .transient_for(root)
                .launch(())
                .forward(sender.input_sender(), identity),
        };

        root.set_titlebar(Some(&input_page.header));
        root.set_child(Some(&container));

        let no_dupes_msg_dlg = MessageDialog::builder()
            .modal(true)
            .message_type(MessageType::Info)
            .transient_for(root)
            .text("No duplicates found")
            .buttons(ButtonsType::Ok)
            .build();

        no_dupes_msg_dlg.connect_response(|dlg, _| dlg.hide());

        let widgets = AppWidgets {
            window: root.clone(),
            container,
            input_page,
            duplicates_page,
            no_dupes_msg_dlg,
        };

        widgets
            .input_page
            .bind(&model.input, sender.input_sender().clone());
        widgets
            .duplicates_page
            .bind(&model.duplicates, sender.input_sender().clone());

        ComponentParts { model, widgets }
    }

    fn update(&mut self, msg: Self::Input, _sender: ComponentSender<Self>) {
        match msg {
            AppMsg::ActivateInputPage => {
                self.selected_page = AppPage::Input;
            }
            AppMsg::AddInputPaths(paths) => {
                for path in paths {
                    self.input.add_path(path);
                }
            }
            AppMsg::UpdateInputPath(position, path) => self.input.update_path(position, path),
            AppMsg::RemoveInputPath(index) => self.input.remove_path(index),
            AppMsg::SetMinSize(value) => self.input.set_min_size(value),
            AppMsg::SetMaxSize(value) => self.input.set_max_size(value),
            AppMsg::FindDuplicates => {
                let config = self.input.group_config();
                self.progress.sender().send(ProgressMsg::Show).unwrap();
                self.group_worker
                    .sender()
                    .send(GroupWorkerMsg::FindDuplicates(config))
                    .unwrap();
            }
            AppMsg::Deduplicate => {
                let progress_sender = self.progress.sender();
                progress_sender.send(ProgressMsg::Progress(None)).unwrap();
                progress_sender
                    .send(ProgressMsg::Message("Removing duplicates...".to_string()))
                    .unwrap();
                progress_sender.send(ProgressMsg::Show).unwrap();

                let groups = self.duplicates.partitioned_groups();
                let op = self.duplicates.dedupe_op().clone();

                self.dedupe_worker
                    .sender()
                    .send(DedupeWorkerMsg::RunDedupe(groups, op))
                    .unwrap()
            }
            AppMsg::SetDedupeOp(op) => self.duplicates.set_dedupe_op(op),
            AppMsg::ClearFiles => {
                self.duplicates.clear_files();
            }
            AppMsg::AddFiles(start_id, files) => {
                self.selected_page = AppPage::Duplicates;
                self.duplicates.add_files(start_id, files);
                self.group_worker
                    .sender()
                    .send(GroupWorkerMsg::GetNextChunk)
                    .unwrap();
            }
            AppMsg::NoDuplicatesFound => {
                self.selected_page = AppPage::NoDuplicatesMsg;
            }
            AppMsg::RemoveFiles(index, files) => {
                self.duplicates.remove_files(index, &files);
            }
            AppMsg::RemoveGroup(index) => self.duplicates.remove_group(index),

            AppMsg::AbortSearch => {
                self.duplicates.clear_files();
            }
            AppMsg::Progress(progress) => self.progress.sender().send(progress).unwrap(),
            AppMsg::ToggleFileSelection(position, selected) => {
                self.duplicates.select_file_at(position, selected);
            }
            AppMsg::SelectAllFiles => self.duplicates.select_all_files(),
            AppMsg::SelectFilesInDirectory(path) => self.duplicates.select_files_in(path),
            AppMsg::UnselectAllFiles => self.duplicates.unselect_all_files(),
            AppMsg::UnselectFilesInDirectory(path) => self.duplicates.unselect_files_in(path),

            AppMsg::SelectionPriorityChanged(priority) => {
                self.duplicates.set_selection_priority(priority)
            }
        }
    }

    fn update_view(&self, widgets: &mut Self::Widgets, sender: ComponentSender<Self>) {
        match self.selected_page {
            AppPage::Input => {
                widgets
                    .container
                    .set_visible_child(&widgets.input_page.root);
                widgets
                    .window
                    .set_titlebar(Some(&widgets.input_page.header));
                widgets.input_page.update(&self.input);
            }
            AppPage::Duplicates => {
                widgets
                    .container
                    .set_visible_child(&widgets.duplicates_page.root);
                widgets
                    .window
                    .set_titlebar(Some(&widgets.duplicates_page.header));
                widgets.duplicates_page.update(&self.duplicates);
            }
            AppPage::NoDuplicatesMsg => {
                widgets.no_dupes_msg_dlg.show();
                sender
                    .input_sender()
                    .send(AppMsg::ActivateInputPage)
                    .unwrap_or_default();
            }
        };
    }
}

fn load_css() {
    let provider = CssProvider::new();
    provider.load_from_data(include_bytes!("style.css"));

    StyleContext::add_provider_for_display(
        &gdk::Display::default().expect("Could not connect to a display."),
        &provider,
        gtk::STYLE_PROVIDER_PRIORITY_APPLICATION,
    );
}
