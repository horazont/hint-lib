import aioxmpp


class Buddies(aioxmpp.service.Service):
    ORDER_AFTER = [aioxmpp.RosterClient]

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self.__buddies = []

    def load_buddies(self, buddies_cfg):
        self.__buddies = []
        for buddy in buddies_cfg:
            self.__buddies.append(
                (
                    aioxmpp.JID.fromstr(buddy["jid"]),
                    set(buddy.get("permissions", []))
                )
            )

    def get_by_permissions(self, keys):
        for jid, perms, *_ in self.__buddies:
            if "*" in perms or (perms & keys) == keys:
                yield jid

    @aioxmpp.service.depsignal(aioxmpp.Client, "on_stream_established")
    def on_stream_established(self):
        roster = self.dependencies[aioxmpp.RosterClient]
        for jid, *_ in self.__buddies:
            roster.approve(jid)
            roster.subscribe(jid)

