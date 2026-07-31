# app/admin/loss/progress_texts.py
# Per phase, 3 items. Each item has text variants for 'low' (0-39), 'mid' (40-69), 'high' (>=70).
# Fill/adjust the wording to your exact sheet; samples given for Phase 1 (from your table).
CATALOG = {
    1: [
        {
            "label": "Adaptation",
            "high": "You can talk about the loss with some comfort. You show confidence in some of your daily tasks.",
            "mid":  "You are not comfortable adapting to changes around you. Other issues not shown here may also be troubling you.",
            "low":  "Clinging to the past is hampering your independence. You are unable to adjust to the loss.",
        },
        {
            "label": "Independence",
            "high": "You’re regaining independence in daily routines.",
            "mid":  "Your independence is inconsistent; support and structure may help.",
            "low":  "Daily independence is significantly affected; additional support is recommended.",
        },
        {
            "label": "Confidence",
            "high": "You demonstrate growing self-confidence in coping situations.",
            "mid":  "Confidence fluctuates; small wins will build stability.",
            "low":  "Confidence is low; targeted coping strategies may be needed.",
        },
    ],
    2: [
        {
            "label": "Emotional Regulation",
            "high": "You are able to stay calm in situations; you show good regulation.",
            "mid":  "You sometimes keep calm, but triggers still unsettle you.",
            "low":  "Staying calm is difficult; regulation skills support is advised.",
        },
        {
            "label": "Understanding",
            "high": "You have a good understanding of your reactions and needs.",
            "mid":  "Understanding is partial; continued reflection can help.",
            "low":  "Limited insight into reactions; guided reflection may help.",
        },
        {
            "label": "Stability",
            "high": "Your day-to-day stability is improving.",
            "mid":  "Some instability remains; routines could assist.",
            "low":  "Stability is affected; safety and support planning advised.",
        },
    ],
    3: [
        {"label": "Connection",   "high": "You lean on supportive connections.", "mid": "Support is uneven.", "low": "Isolation is a risk; connect with supports."},
        {"label": "Expression",   "high": "You can express feelings clearly.",   "mid": "Expression is hesitant.", "low": "Expression is restricted; safe spaces needed."},
        {"label": "Acceptance",   "high": "Signs of acceptance are emerging.",   "mid": "Acceptance is mixed.", "low": "Acceptance is not yet evident."},
    ],
    4: [
        {"label": "Routine",      "high": "Healthy routines are re-establishing.", "mid": "Routines are inconsistent.", "low": "Routines are disrupted; structure helps."},
        {"label": "Functioning",  "high": "Daily functioning is adequate.",        "mid": "Functioning varies day-to-day.", "low": "Functioning is impaired; assistance advised."},
        {"label": "Motivation",   "high": "Motivation is returning.",              "mid": "Motivation fluctuates.", "low": "Motivation is low; stepwise goals help."},
    ],
    5: [
        {"label": "Hope",         "high": "Hopefulness is evident.",               "mid": "Hope is tentative.", "low": "Hope feels distant; nurture small positives."},
        {"label": "Future Plans", "high": "You’re planning ahead realistically.",  "mid": "Plans exist but feel fragile.", "low": "Planning is difficult at present."},
        {"label": "Resilience",   "high": "Resilience is strengthening.",          "mid": "Resilience is developing.", "low": "Resilience is strained; support needed."},
    ],
}
