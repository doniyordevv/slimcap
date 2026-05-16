mod cdr;
mod cli;
mod detect;
mod encoder;
mod ordered;
mod pb;
mod pipeline;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    ffmpeg_next::init()?;
    ffmpeg_next::log::set_level(ffmpeg_next::log::Level::Error);

    let args = cli::Args::parse();
    pipeline::run(args)
}
