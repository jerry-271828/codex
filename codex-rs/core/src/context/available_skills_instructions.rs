use codex_core_skills::AvailableSkills;
use codex_core_skills::SKILLS_HOW_TO_USE_WITH_ABSOLUTE_PATHS;
use codex_core_skills::SKILLS_HOW_TO_USE_WITH_ALIASES;
use codex_core_skills::render_available_skills_body;
use codex_protocol::protocol::SKILLS_INSTRUCTIONS_CLOSE_TAG;
use codex_protocol::protocol::SKILLS_INSTRUCTIONS_OPEN_TAG;
use codex_utils_output_truncation::approx_bytes_for_tokens;
use codex_utils_output_truncation::approx_token_count;

use super::ContextualUserFragment;

const MAX_SKILLS_CONTEXT_FRAGMENT_TOKENS: usize = 9_000;
const SKILLS_CONTEXT_MARKER_BYTES: usize =
    SKILLS_INSTRUCTIONS_OPEN_TAG.len() + SKILLS_INSTRUCTIONS_CLOSE_TAG.len();

/// Model-context fragment describing the skills available to Codex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableSkillsInstructions {
    skill_root_lines: Vec<String>,
    skill_lines: Vec<String>,
}

impl AvailableSkillsInstructions {
    /// Creates a skills context fragment from pre-rendered catalog lines.
    pub fn from_skill_lines(skill_lines: Vec<String>) -> Self {
        Self {
            skill_root_lines: Vec::new(),
            skill_lines,
        }
    }

    pub fn from_available_skills(
        available_skills: &AvailableSkills,
        include_skills_usage_instructions: bool,
    ) -> Self {
        let mut skill_lines = available_skills.skill_lines.clone();
        if include_skills_usage_instructions {
            skill_lines.push("### How to use skills".to_string());
            let instructions = if available_skills.skill_root_lines.is_empty() {
                SKILLS_HOW_TO_USE_WITH_ABSOLUTE_PATHS
            } else {
                SKILLS_HOW_TO_USE_WITH_ALIASES
            };
            skill_lines.push(instructions.to_string());
        }
        Self {
            skill_root_lines: available_skills.skill_root_lines.clone(),
            skill_lines,
        }
    }

    pub(crate) fn render_chunks(&self) -> Vec<String> {
        let body = self.body();
        let render_chunk = |body: &str| {
            format!("{SKILLS_INSTRUCTIONS_OPEN_TAG}{body}{SKILLS_INSTRUCTIONS_CLOSE_TAG}")
        };
        if approx_token_count(&render_chunk(&body)) <= MAX_SKILLS_CONTEXT_FRAGMENT_TOKENS {
            return vec![render_chunk(&body)];
        }

        let mut chunks = Vec::new();
        let mut current = String::new();
        for line in body.split_inclusive('\n') {
            let mut candidate = current.clone();
            candidate.push_str(line);
            if !current.is_empty()
                && approx_token_count(&render_chunk(&candidate))
                    > MAX_SKILLS_CONTEXT_FRAGMENT_TOKENS
            {
                chunks.push(render_chunk(&current));
                current.clear();
            }
            let mut remaining = line;
            while approx_token_count(&render_chunk(remaining)) > MAX_SKILLS_CONTEXT_FRAGMENT_TOKENS
            {
                let max_body_bytes = approx_bytes_for_tokens(MAX_SKILLS_CONTEXT_FRAGMENT_TOKENS)
                    .saturating_sub(SKILLS_CONTEXT_MARKER_BYTES);
                let split_at = remaining
                    .char_indices()
                    .take_while(|(index, character)| {
                        index.saturating_add(character.len_utf8()) <= max_body_bytes
                    })
                    .map(|(index, character)| index + character.len_utf8())
                    .last()
                    .unwrap_or(remaining.len());
                let (prefix, suffix) = remaining.split_at(split_at);
                chunks.push(render_chunk(prefix));
                remaining = suffix;
            }
            current.push_str(remaining);
        }
        if !current.is_empty() {
            chunks.push(render_chunk(&current));
        }
        chunks
    }
}

#[cfg(test)]
#[path = "available_skills_instructions_tests.rs"]
mod tests;

impl ContextualUserFragment for AvailableSkillsInstructions {
    fn role(&self) -> &'static str {
        "developer"
    }

    fn markers(&self) -> (&'static str, &'static str) {
        Self::type_markers()
    }

    fn type_markers() -> (&'static str, &'static str) {
        (SKILLS_INSTRUCTIONS_OPEN_TAG, SKILLS_INSTRUCTIONS_CLOSE_TAG)
    }

    fn body(&self) -> String {
        render_available_skills_body(&self.skill_root_lines, &self.skill_lines)
    }
}
