"""
Multi-provider AI helper for the GCP Data & AI CoE demo.

Lazy imports each provider's SDK only when actually called, so the base app
doesn't require anthropic/openai/google-generativeai to be installed unless
this feature is used. Install with:
    pip install -r requirements-ai.txt

API keys are supplied by the user at runtime (via the app's UI) and are
never written to disk or logged - they live only in Streamlit's
st.session_state for the duration of the browser session.
"""

import logging

logger = logging.getLogger("gcp_coe_ai_helper")

# Default model names as of this app's last update. Provider model
# lineups change frequently - if a call fails with a "model not found"
# style error, check the provider's current docs and update the model
# name in the app's UI (it's an editable field, not hardcoded).
DEFAULT_MODELS = {
    "Claude (Anthropic)": "claude-sonnet-5",
    "ChatGPT (OpenAI)": "gpt-4o",
    "Gemini (Google)": "gemini-1.5-pro",
}


def call_claude(api_key, model, messages, system=None):
    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        system=system or "",
        messages=[{"role": m["role"], "content": m["content"]} for m in messages],
    )
    return "".join(block.text for block in response.content if hasattr(block, "text"))


def call_chatgpt(api_key, model, messages, system=None):
    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    full_messages = ([{"role": "system", "content": system}] if system else []) + messages
    response = client.chat.completions.create(model=model, messages=full_messages)
    return response.choices[0].message.content


def call_gemini(api_key, model, messages, system=None):
    import google.generativeai as genai
    genai.configure(api_key=api_key)
    gm = genai.GenerativeModel(model, system_instruction=system or None)
    # Gemini's chat API expects alternating user/model turns; map our
    # "assistant" role to "model" and drop system (handled above).
    history = [
        {"role": "user" if m["role"] == "user" else "model", "parts": [m["content"]]}
        for m in messages[:-1]
    ]
    chat = gm.start_chat(history=history)
    response = chat.send_message(messages[-1]["content"])
    return response.text


PROVIDER_FUNCS = {
    "Claude (Anthropic)": call_claude,
    "ChatGPT (OpenAI)": call_chatgpt,
    "Gemini (Google)": call_gemini,
}


def ask(provider, api_key, model, messages, system=None):
    """Dispatches to the right provider. Raises on failure - caller should
    catch and show a friendly error (missing key, bad model name, network,
    or the SDK not being installed)."""
    func = PROVIDER_FUNCS[provider]
    return func(api_key, model, messages, system=system)
