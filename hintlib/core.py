import aioxmpp

from .services import Buddies


class BotCore:
    def __init__(self, xmpp_config, client_logger=None):
        super().__init__()
        self.__config = xmpp_config
        self.client = aioxmpp.Client(
            aioxmpp.JID.fromstr(xmpp_config["jid"]),
            aioxmpp.make_security_layer(
                xmpp_config["password"]
            ),
            logger=client_logger,
        )

        self.__nested_cm = None

        self.buddies = self.client.summon(Buddies)
        self.buddies.load_buddies(xmpp_config.get("buddies", []))

    async def __aenter__(self):
        self.__nested_cm = self.client.connected(
            presence=aioxmpp.PresenceState(True),
        )
        return (await self.__nested_cm.__aenter__())

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        return (await self.__nested_cm.__aexit__(
            exc_type,
            exc_value,
            exc_traceback,
        ))

