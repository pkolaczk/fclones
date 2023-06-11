use adw::gtk;

use adw::prelude::*;
use gtk::glib;

const UNLIMITED_STR: &str = "Unlimited";

/** A text field with a slider allowing to enter bytes value. */
pub struct BytesRow {
    row: adw::ActionRow,
    entry: gtk::EditableLabel,
    default: String,
}

impl BytesRow {
    pub fn new(title: &str, subtitle: &str, default: &str, unlimited: bool) -> BytesRow {
        let entry = gtk::EditableLabel::builder()
            .xalign(1.0)
            .valign(gtk::Align::Center)
            .halign(gtk::Align::End)
            .hexpand(false)
            .build();
        let adjustment = gtk::Adjustment::builder()
            .lower(0.0)
            .upper(12.0)
            .page_increment(1.0)
            .step_increment(0.05)
            .build();
        let scale = gtk::Scale::new(gtk::Orientation::Horizontal, Some(&adjustment));
        scale.set_width_request(240);

        bind(&scale, &entry, unlimited);
        entry.set_text(default);

        let row = adw::ActionRow::builder()
            .title(title)
            .subtitle(subtitle)
            .build();
        row.add_suffix(&entry);
        row.add_suffix(&scale);
        row.set_activatable_widget(Some(&entry));
        row.connect_activate(glib::clone!(@strong entry => move |_| {
            entry.grab_focus();
            entry.start_editing();
        }));

        BytesRow {
            row,
            entry,
            default: default.to_owned(),
        }
    }

    pub fn row(&self) -> &adw::ActionRow {
        &self.row
    }

    pub fn set_value(&self, bytes: u64) {
        set_bytes(bytes, &self.entry);
    }

    pub fn on_change(&self, action: impl Fn(u64) + 'static) {
        let default_value = self.default.clone();
        // Rewrite the value to use nicer byte units
        self.entry.connect_editing_notify(move |entry| {
            if entry.is_editing() {
                let text = entry.text();
                entry.set_text(text.as_str());
                entry.select_region(0, text.len() as i32);
            }

            // This will trigger only after the user stops editing the label entry
            if !entry.is_editing() {
                match parse_size::parse_size(entry.text()) {
                    Ok(bytes) => set_bytes(bytes, entry),
                    Err(_) => entry.set_text(default_value.as_str()),
                }
            }
        });
        // Invoke the action on every text entry change:
        self.entry.connect_changed(move |entry| {
            if entry.text() == UNLIMITED_STR {
                action(u64::MAX)
            } else if let Ok(bytes) = parse_size::parse_size(entry.text()) {
                action(bytes);
            }
        });
    }
}

fn set_bytes(bytes: u64, entry: &gtk::EditableLabel) {
    if !entry.is_editing() {
        let value_str = if bytes == u64::MAX {
            UNLIMITED_STR.to_owned()
        } else {
            let bytes = byte_unit::Byte::from_bytes(bytes as u128);
            let adjusted = bytes.get_appropriate_unit(false);
            adjusted.get_value();
            format!("{} {}", adjusted.get_value(), adjusted.get_unit())
        };
        if entry.text() != value_str.as_str() {
            entry.set_text(value_str.as_str());
        }
    }
}

fn bind(scale: &gtk::Scale, entry: &gtk::EditableLabel, unlimited: bool) {
    scale.connect_change_value(
        glib::clone!(@weak entry => @default-panic, move |scale, _, value| {
            if unlimited && value == scale.adjustment().upper() {
                entry.set_text(UNLIMITED_STR);
            }
            else {
                let bytes = scale_to_bytes(value);
                set_bytes(bytes, &entry);
            }
            gtk::Inhibit(false)
        }),
    );
    entry.connect_changed(glib::clone!(@weak scale => @default-panic, move |value| {
        let text = value.text();
        if unlimited && text == UNLIMITED_STR {
            scale.set_value(scale.adjustment().upper());
        }
        else if let Ok(bytes) = parse_size::parse_size(text) {
            scale.set_value(bytes_to_scale(bytes));
        }
    }));
}

fn bytes_to_scale(bytes: u64) -> f64 {
    let round_to = 0.05;
    (((bytes + 1) as f64).log10() / round_to).round() * round_to
}

fn scale_to_bytes(value: f64) -> u64 {
    let mut bytes = 10.0f64.powf(value);
    if bytes < 10.0 {
        bytes = (bytes - 1.0) * (10.0 / 9.0);
    }
    round(bytes, 2)
}

fn round(x: f64, n: u32) -> u64 {
    let d = x.log10().ceil();
    let power = n as f64 - d;

    let magnitude = 10f64.powf(power);
    let shifted = (x * magnitude).round();

    (shifted / magnitude).round() as u64
}
