# Future Plans: Prestige Scoring System

## Overview
The goal is to implement a "Prestige Score" (1-5) for each extracurricular program using Gemini's reasoning capabilities and internal knowledge.

## Proposed Rubric
- **Level 5 (Elite):** Globally recognized, extremely selective (<5% acceptance), free or fully funded (e.g., RSI, TASP, MITES, SSP).
- **Level 4 (Highly Competitive):** Nationally recognized, selective application, hosted by top-tier universities (Ivy League, Stanford, MIT, top state schools).
- **Level 3 (Competitive):** Strong academic enrichment, standard application process, hosted by reputable colleges or organizations.
- **Level 2 (Enrichment):** Primarily "pay-to-play" or attendance-based programs with standard requirements.
- **Level 1 (Local/Recreational):** Local programs, workshops, or non-academic camps.

## Implementation Strategy (Step 7)
1. **Model Selection:** Use `gemini-3-flash-preview` or `gemini-1.5-pro` for higher reasoning.
2. **Inputs:**
   - Program Title & Subtitle (from Step 6).
   - Combined Markdown Content (as evidence of selectivity, capacity, and requirements).
   - Internal Knowledge (instruct the model to use its training data on rankings and reputation).
3. **Prompting:**
   - Use a strict JSON output format.
   - Provide the rubric explicitly in the system prompt.
   - Ask for a brief "justification" field to explain why the score was given.

## Challenges to Address
- **Hallucination:** Guard against the model over-scoring programs simply because they are hosted at prestigious universities (e.g., a sports camp at Harvard should not get a 5).
- **Consistency:** Use a `temperature` of 0.0 to ensure reproducible scores across multiple runs.
- **Missing Data:** For obscure or local programs, the model should default to a lower score rather than guessing high.

