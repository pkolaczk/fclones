use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use adw::prelude::*;
use relm4::gtk::prelude::GtkWindowExt;
use relm4::{gtk, view, Component, ComponentParts, ComponentSender, RelmWidgetExt, Sender};

use fclones::log::{Log, LogLevel, ProgressBarLength};
use fclones::progress::ProgressTracker;

use crate::app::AppMsg;

pub struct ProgressDialog {
    visible: bool,
    message: String,
    progress: Option<f64>,
}

pub struct ProgressWidgets {
    dialog: adw::Window,
    label: gtk::Label,
    progress: gtk::ProgressBar,
}

#[derive(Clone, Debug)]
pub enum ProgressMsg {
    Show,
    Hide,
    Abort,
    Progress(Option<f64>),
    Message(String),
}

impl Component for ProgressDialog {
    type CommandOutput = ();
    type Input = ProgressMsg;
    type Output = AppMsg;
    type Init = ();
    type Root = adw::Window;
    type Widgets = ProgressWidgets;

    fn init_root() -> Self::Root {
        let dialog = adw::Window::default();
        dialog.set_modal(true);
        dialog.set_title(Some("Searching for duplicates"));
        dialog.set_resizable(false);
        dialog
    }

    fn init(
        _init: Self::Init,
        root: &Self::Root,
        sender: ComponentSender<Self>,
    ) -> ComponentParts<Self> {
        let model = ProgressDialog {
            visible: false,
            message: "Running".to_owned(),
            progress: None,
        };

        view! {
            #[name = "container"]
            gtk::Box {
                set_orientation: gtk::Orientation::Vertical,
                set_margin_all: 20,
                set_spacing: 10,

                #[name = "label"]
                gtk::Label {
                    set_width_request: 600,
                    set_height_request: 20,
                    set_halign: gtk::Align::Center,
                    set_valign: gtk::Align::Center,
                },

                #[name = "progress"]
                gtk::ProgressBar {
                    set_width_request: 600,
                    set_halign: gtk::Align::Center,
                    set_valign: gtk::Align::Center,
                }
            }
        }

        root.set_content(Some(&container));
        root.connect_close_request(move |_| {
            sender.input(ProgressMsg::Abort);
            gtk::Inhibit(false)
        });

        let widgets = ProgressWidgets {
            dialog: root.clone(),
            progress,
            label,
        };
        ComponentParts { model, widgets }
    }

    fn update(&mut self, msg: Self::Input, sender: ComponentSender<Self>, _root: &Self::Root) {
        match msg {
            ProgressMsg::Show => self.visible = true,
            ProgressMsg::Hide => self.visible = false,
            ProgressMsg::Abort => sender.output(AppMsg::AbortSearch).unwrap(),
            ProgressMsg::Message(s) => self.message = s,
            ProgressMsg::Progress(f) => self.progress = f,
        }
    }

    fn update_cmd(
        &mut self,
        _message: Self::CommandOutput,
        _sender: ComponentSender<Self>,
        _root: &Self::Root,
    ) {
    }

    fn update_view(&self, widgets: &mut Self::Widgets, _sender: ComponentSender<Self>) {
        widgets.dialog.set_visible(self.visible);
        widgets.label.set_label(self.message.as_str());
        match self.progress {
            Some(fraction) => widgets.progress.set_fraction(fraction),
            None => widgets.progress.pulse(),
        }
    }
}

pub struct ProgressBar {
    count: AtomicU64,
    len: Option<u64>,
}

impl ProgressBar {
    pub fn new(len: Option<u64>) -> ProgressBar {
        Self {
            count: AtomicU64::new(0),
            len,
        }
    }
}

impl ProgressBar {
    pub fn progress(&self) -> Option<f64> {
        self.len
            .map(|len| (self.count.load(Ordering::Relaxed) as f64) / (len as f64))
    }
}

impl ProgressTracker for ProgressBar {
    fn inc(&self, delta: u64) {
        self.count.fetch_add(delta, Ordering::Relaxed);
    }
}

pub struct LogAdapter {
    sender: Sender<ProgressMsg>,
}

impl LogAdapter {
    pub fn new(sender: Sender<ProgressMsg>) -> Self {
        Self { sender }
    }
}

impl Log for LogAdapter {
    fn progress_bar(&self, msg: &str, len: ProgressBarLength) -> Arc<dyn ProgressTracker> {
        self.sender
            .send(ProgressMsg::Message(msg.to_owned()))
            .unwrap_or_default();
        let progress_bar = match len {
            ProgressBarLength::Items(n) => Arc::new(ProgressBar::new(Some(n))),
            ProgressBarLength::Bytes(n) => Arc::new(ProgressBar::new(Some(n))),
            ProgressBarLength::Unknown => Arc::new(ProgressBar::new(None)),
        };
        let weak_pb = Arc::downgrade(&progress_bar);
        let sender = self.sender.clone();
        thread::spawn(move || {
            while let Some(pb) = weak_pb.upgrade() {
                sender
                    .send(ProgressMsg::Progress(pb.progress()))
                    .unwrap_or_default();
                thread::sleep(Duration::from_millis(50));
            }
        });
        progress_bar
    }

    fn log(&self, _level: LogLevel, _msg: String) {}
}
