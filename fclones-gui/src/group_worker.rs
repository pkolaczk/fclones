use crate::app::AppMsg;
use crate::progress::{LogAdapter, ProgressMsg};

use fclones::config::GroupConfig;
use fclones::PathAndMetadata;
use relm4::{spawn, ComponentSender, Worker};
use std::collections::VecDeque;
use std::time::SystemTime;

pub struct GroupWorker {
    group_start_id: usize,
    groups: VecDeque<Vec<fclones::FileGroup<fclones::FileInfo>>>,
    start_time: SystemTime,
}

impl GroupWorker {
    fn find_duplicates(&mut self, config: GroupConfig, sender: ComponentSender<Self>) {
        sender.output(AppMsg::ClearFiles).unwrap_or_default();

        let (tx, rx) = relm4::channel();
        let log = LogAdapter::new(tx);
        spawn(rx.forward(sender.output_sender().clone(), AppMsg::Progress));

        let groups = fclones::group_files(&config, &log);
        if let Ok(groups) = groups {
            self.groups = VecDeque::new();
            self.group_start_id = 0;
            for chunk in groups.chunks(256) {
                self.groups.push_back(chunk.to_vec())
            }
            sender
                .output(AppMsg::Progress(ProgressMsg::Hide))
                .unwrap_or_default();
            self.start_time = SystemTime::now();
            if groups.is_empty() {
                sender.output(AppMsg::NoDuplicatesFound).unwrap_or_default();
            } else {
                self.send_next_chunk(sender)
            }
        }
    }

    fn send_next_chunk(&mut self, sender: ComponentSender<Self>) {
        if let Some(chunk) = self.groups.pop_front() {
            let chunk_len = chunk.len();
            let chunk = chunk
                .iter()
                .cloned()
                .map(|group| group.filter_map(|f| PathAndMetadata::new(f.path).ok()))
                .collect();
            sender
                .output(AppMsg::AddFiles(self.group_start_id, chunk))
                .unwrap_or_default();
            self.group_start_id += chunk_len;
        }
    }
}

#[derive(Debug)]
pub enum GroupWorkerMsg {
    FindDuplicates(fclones::config::GroupConfig),
    GetNextChunk,
}

impl Worker for GroupWorker {
    type Init = ();
    type Input = GroupWorkerMsg;
    type Output = AppMsg;

    fn init(_init: Self::Init, _sender: ComponentSender<Self>) -> Self {
        GroupWorker {
            group_start_id: 0,
            groups: VecDeque::new(),
            start_time: SystemTime::now(),
        }
    }

    fn update(&mut self, msg: GroupWorkerMsg, sender: ComponentSender<Self>) {
        match msg {
            GroupWorkerMsg::FindDuplicates(config) => self.find_duplicates(config, sender),
            GroupWorkerMsg::GetNextChunk => self.send_next_chunk(sender),
        }
    }
}
