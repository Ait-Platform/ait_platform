class LunaGateway:
    """UIP AI is unavailable until a real integration is implemented."""

    @staticmethod
    def ask_luna(prompt, interaction_id=None):
        # No provisioning, charging or usage logging for simulated AI.
        return {
            "status": "unavailable",
            "message": "AI unavailable in this prototype."
        }
