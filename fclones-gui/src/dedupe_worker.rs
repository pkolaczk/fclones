use std::cmp;
use std::collections::HashMap;
use std::sync::Mutex;

use rayon::iter::ParallelBridge;
use rayon::iter::ParallelIterator;
use relm4::{spawn, ComponentSender, Worker};

use fclones::log::{Log, ProgressBarLength};
use fclones::{DedupeOp, PartitionedFileGroup};

use crate::app::AppMsg;
use crate::progress::{LogAdapter, ProgressMsg};

pub struct DedupeWorker {}

impl DedupeWorker {
    pub fn dedupe(
        &mut self,
        groups: Vec<PartitionedFileGroup>,
        op: DedupeOp,
        sender: ComponentSender<Self>,
    ) {
        let devices = fclones::DiskDevices::new(&HashMap::new());
        let (tx, rx) = relm4::channel();
        let log = LogAdapter::new(tx);
        spawn(rx.forward(sender.output_sender().clone(), AppMsg::Progress));

        // Remembers which groups should be removed from the duplicates view.
        // We cannot remove them as we go, because removing shifts the indexes.
        let removed_groups = Mutex::new(Vec::new());
        let progress = log.progress_bar(
            "Removing duplicates...",
            ProgressBarLength::Items(groups.len() as u64),
        );

        groups
            .into_iter()
            .enumerate()
            .par_bridge()
            .for_each_with(progress, |progress, (i, g)| {
                let group_size = g.to_keep.len() + g.to_drop.len();
                let mut removed_files = Vec::new();
                let commands = g.dedupe_script(&op, &devices);
                for cmd in commands {
                    if cmd.execute(false, &log).is_ok() {
                        removed_files.push(cmd.file_to_remove().clone());
                    }
                }
                progress.inc(1);

                if group_size - removed_files.len() < 2 {
                    removed_groups.lock().unwrap().push(i as u32);
                } else {
                    sender
                        .output(AppMsg::RemoveFiles(i as u32, removed_files))
                        .unwrap();
                }
            });

        let mut removed_groups = removed_groups.into_inner().unwrap();
        removed_groups.sort_by_key(|&v| cmp::Reverse(v));
        for i in removed_groups {
            sender.output(AppMsg::RemoveGroup(i)).unwrap();
        }

        sender.output(AppMsg::Progress(ProgressMsg::Hide)).unwrap();
    }
}

#[derive(Debug)]
pub enum DedupeWorkerMsg {
    RunDedupe(Vec<PartitionedFileGroup>, DedupeOp),
}

impl Worker for DedupeWorker {
    type Init = ();
    type Input = DedupeWorkerMsg;
    type Output = AppMsg;

    fn init(_init: Self::Init, _sender: ComponentSender<Self>) -> Self {
        DedupeWorker {}
    }

    fn update(&mut self, msg: DedupeWorkerMsg, sender: ComponentSender<Self>) {
        match msg {
            DedupeWorkerMsg::RunDedupe(groups, op) => self.dedupe(groups, op, sender),
        }
    }
}
