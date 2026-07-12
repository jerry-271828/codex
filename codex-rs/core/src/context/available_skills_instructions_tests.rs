use codex_protocol::protocol::SKILLS_INSTRUCTIONS_CLOSE_TAG;
use codex_protocol::protocol::SKILLS_INSTRUCTIONS_OPEN_TAG;
use codex_utils_output_truncation::approx_token_count;

use super::AvailableSkillsInstructions;
use super::MAX_SKILLS_CONTEXT_FRAGMENT_TOKENS;
use crate::context::ContextualUserFragment;

#[test]
fn rendered_chunks_preserve_body_and_stay_bounded() {
    let mut skill_lines = (0..100)
        .map(|index| format!("- skill-{index}: {}", "x".repeat(1_000)))
        .collect::<Vec<_>>();
    skill_lines.push(format!("- unicode-skill: {}", "💡".repeat(10_000)));
    let instructions = AvailableSkillsInstructions::from_skill_lines(skill_lines);

    let chunks = instructions.render_chunks();

    assert!(chunks.len() > 1);
    assert!(
        chunks
            .iter()
            .all(|chunk| approx_token_count(chunk) <= MAX_SKILLS_CONTEXT_FRAGMENT_TOKENS)
    );
    let combined_body = chunks
        .iter()
        .map(|chunk| {
            chunk
                .strip_prefix(SKILLS_INSTRUCTIONS_OPEN_TAG)
                .and_then(|chunk| chunk.strip_suffix(SKILLS_INSTRUCTIONS_CLOSE_TAG))
                .expect("chunk should retain skills context markers")
        })
        .collect::<String>();
    assert_eq!(combined_body, instructions.body());
}
