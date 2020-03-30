import aioxmpp

from .services import Buddies


class BotCore:
    def __init__(self, xmpp_config, client_logger=None):
        super().__init__()
        self.__config = xmpp_config

        override_peer = []
        if xmpp_config.get("host"):
            override_peer.append(
                (xmpp_config["host"],
                 xmpp_config.get("port", 5222),
                 aioxmpp.connector.STARTTLSConnector())
            )

        security_args = {}
        if xmpp_config.get("public_key_pin"):
            security_args["pin_store"] = xmpp_config["public_key_pin"]
            security_args["pin_type"] = aioxmpp.security_layer.PinType.PUBLIC_KEY

        self.client = aioxmpp.Client(
            aioxmpp.JID.fromstr(xmpp_config["jid"]),
            aioxmpp.make_security_layer(
                xmpp_config["password"],
                **security_args,
            ),
            override_peer=override_peer,
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

