/// Identifies a phase of work.
/// Used for reporting / progress tracking.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Phase {
    Walk,
    FetchExtents,
    GroupBySize,
    GroupByPrefix,
    GroupBySuffix,
    GroupByContents,
    TransformAndGroup,
}

impl Phase {
    pub fn name(&self) -> &'static str {
        match self {
            Phase::Walk => "Scanning files",
            Phase::FetchExtents => "Fetching extends",
            Phase::GroupBySize => "Grouping by size",
            Phase::GroupByPrefix => "Grouping by prefix",
            Phase::GroupBySuffix => "Grouping by suffix",
            Phase::GroupByContents => "Grouping by contents",
            Phase::TransformAndGroup => "Transforming and grouping",
        }
    }
}

/// Represents a sequence of phases.
/// Used for progress reporting.
pub struct Phases(Vec<Phase>);

impl Phases {
    pub fn new(phases: Vec<Phase>) -> Phases {
        Phases(phases)
    }

    /// Returns a string with the sequential number of the phase and its name.
    /// Panics if the vector does not contain given phase.
    pub fn format(&self, phase: Phase) -> String {
        let phase_no = self.0.iter().position(|p| *p == phase).unwrap();
        let phase_count = self.0.len();
        format!("{}/{}: {}", phase_no + 1, phase_count, phase.name())
    }
}

#[cfg(test)]
mod test {
    use crate::phase::{Phase, Phases};

    #[test]
    fn format_phase() {
        let phases = Phases(vec![
            Phase::Walk,
            Phase::GroupBySize,
            Phase::GroupByPrefix,
            Phase::GroupBySuffix,
            Phase::GroupByContents,
        ]);
        assert_eq!("1/5: Scanning files", phases.format(Phase::Walk));
        assert_eq!("2/5: Grouping by size", phases.format(Phase::GroupBySize));
    }
}
