//! Skill-related configuration types shared across crates.

use std::num::NonZeroUsize;

use codex_utils_absolute_path::AbsolutePathBuf;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;

const MAX_SKILL_CONTEXT_BUDGET_TOKENS: usize = 100_000;

const fn default_enabled() -> bool {
    true
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SkillConfig {
    /// Path-based selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<AbsolutePathBuf>,
    /// Name-based selector.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub enabled: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct SkillsConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundled: Option<BundledSkillsConfig>,

    /// Whether turns receive the automatic skills instructions block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_instructions: Option<bool>,

    /// Fixed aggregate token budget for model-visible host skill metadata.
    #[serde(
        default,
        deserialize_with = "deserialize_context_budget_tokens",
        skip_serializing_if = "Option::is_none"
    )]
    #[schemars(range(min = 1, max = 100000))]
    pub context_budget_tokens: Option<NonZeroUsize>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<SkillConfig>,
}

fn deserialize_context_budget_tokens<'de, D>(
    deserializer: D,
) -> Result<Option<NonZeroUsize>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<NonZeroUsize>::deserialize(deserializer)?;
    if value.is_some_and(|value| value.get() > MAX_SKILL_CONTEXT_BUDGET_TOKENS) {
        return Err(serde::de::Error::custom(format!(
            "skills.context_budget_tokens must not exceed {MAX_SKILL_CONTEXT_BUDGET_TOKENS}"
        )));
    }
    Ok(value)
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct BundledSkillsConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl Default for BundledSkillsConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

impl TryFrom<toml::Value> for SkillsConfig {
    type Error = toml::de::Error;

    fn try_from(value: toml::Value) -> Result<Self, Self::Error> {
        SkillsConfig::deserialize(value)
    }
}
