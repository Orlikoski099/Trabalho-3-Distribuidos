import Pyro5.api
import time
import threading
from datetime import datetime


class Broker:
    def __init__(self, broker_id, state):
        """
        Inicializa um broker com ID, estado e estrutura para heartbeat e log.
        """
        self.committed_log = []  # Lista de dados confirmados
        self.broker_id = broker_id
        self.state = state  # 'lider', 'votante' ou 'observador'
        self.heartbeat_status = {}  # Dicionário de heartbeats {broker_id: count}
        # Dicionário de logs {message: [broker_id1, broker_id2, ...]}
        self.quorum = {}
        self.log2 = []

        self.members = []  # Lista de membros do cluster
        # Confirmações de dados {data_key: [voter_id1, ...]}
        self.confirmations = {}
        self.lock = threading.Lock()

    # Lider
    @Pyro5.api.expose
    def register_member(self, uri, member_id, member_state):
        """
        Registra um novo membro no cluster.
        :param uri: URI do membro.
        :param member_id: ID do membro.
        :param member_state: Estado do membro ('votante' ou 'observador').
        """
        self.members.append(
            {"uri": uri, "id": member_id, "state": member_state})
        print(f"Membro {member_id} com estado '{member_state}' registrado.")

    # Lider
    @Pyro5.api.expose
    def append_log(self, data):
        """
        Adiciona um dado ao log e notifica os votantes.
        :param data: Dado a ser adicionado.
        """
        if data not in self.quorum:
            self.quorum[data] = []  # Inicializa a mensagem com lista vazia
            self.log2.append(data)
        self.notify_voters(data)
        print(f"[Broker {self.broker_id}] Dado '{data}' adicionado ao log.")

    # Lider
    def notify_voters(self, data):
        """
        Notifica votantes sobre um novo dado.
        :param data: Dado a ser notificado.
        """
        for member in self.members:
            voter_obj = Pyro5.api.Proxy(member["uri"])
            print(f"\n\n\n {member['id']} \n\n\n")
            voter_obj.update_log(self.log2.index(data))
            print(f"Notificação enviada para votante {member['id']}.")

    # Voter
    def fetch_log(self, offset):
        lider = self.get_lider()
        log = lider.get_uncommited_data(offset)
        return log

    # Voter
    @Pyro5.api.expose
    def update_log(self, offset):
        """
        Atualiza o log local e confirma recebimento ao lider.
        :param data: Dado recebido.
        """
        data = self.fetch_log(offset)
        self.log2.append(data)
        if (self.state == "votante"):
            lider = self.get_lider()
            lider.confirm_consume(self.broker_id, data)

    # Lider
    @Pyro5.api.expose
    def confirm_consume(self, voter_id, data):
        """
        Registra confirmação de recebimento de dado por votante.
        :param voter_id: ID do votante.
        :param data: Dado confirmado.
        """
        print("\n\n\nTESTE1\n\n\n")
        if data not in self.confirmations:
            self.confirmations[data] = []
        print("\n\n\nTESTE2\n\n\n")
        if voter_id not in self.confirmations[data]:
            self.confirmations[data].append(voter_id)
            print(f"Broker {voter_id} confirmou o dado: {data}")
        print("\n\n\nTESTE3\n\n\n")

        # Verifica quórum
        if len(self.confirmations[data]) >= -(-len(self.members) / 2):
            self.committed_log.append(data)
            del self.confirmations[data]  # Remove confirmações do dado
            print(f"Dado '{data}' confirmado pelo quórum!")

    # Voter
    def get_lider(self):
        """
        Obtém o proxy do lider.
        """
        return Pyro5.api.Proxy("PYRONAME:Lider-Epoca1")

    # Lider
    @Pyro5.api.expose
    def recv_heartbeat(self, broker_id, state):
        """
        Recebe um heartbeat e atualiza o timestamp do último recebimento.
        :param broker_id: ID do broker enviando o heartbeat.
        :param state: Estado do broker.
        """
        with self.lock:
            self.heartbeat_status[broker_id] = datetime.now()
            print(
                f"Heartbeat recebido de {broker_id} em {self.heartbeat_status[broker_id]}")

    # Votante
    def send_heartbeat(self):
        """
        Envia um heartbeat periodicamente ao lider.
        """
        while True:
            time.sleep(3)
            self.get_lider().recv_heartbeat(self.broker_id, self.state)

    # Lider
    def check_heartbeats(self):
        """
        Verifica se algum broker falhou com base no tempo desde o último heartbeat.
        Substitui votantes inoperantes por observadores.
        """
        while True:
            time.sleep(5)
            with self.lock:
                now = datetime.now()
                brokers_to_remove = []

                for broker_id, last_heartbeat in self.heartbeat_status.items():
                    time_diff = (now - last_heartbeat).total_seconds()
                    if time_diff > 10:  # Mais de 10 segundos sem heartbeat
                        print(
                            f"Broker votante {broker_id} falhou! Último heartbeat há {time_diff:.1f} segundos.")
                        brokers_to_remove.append(broker_id)
                        self.promote_observer()  # Promove um observador
                        self.members = [
                            member for member in self.members if member["id"] != broker_id]

                for broker_id in brokers_to_remove:
                    self.heartbeat_status.pop(broker_id, None)

    # Voter
    @Pyro5.api.expose
    def reset_log(self, new_log):
        self.log2 = new_log

    # Lider
    @Pyro5.api.expose
    def get_uncommited_data(self, offset):
        return self.log2[offset]

    # Lider
    @Pyro5.api.expose
    def get_log(self, offset):
        """
        Retorna o log do broker (chamado pelo consumidor).
        :return: Log do broker.
        """
        if int(offset) > len(self.committed_log) or int(offset) < 0:
            return print("fora dos limites")
        return self.committed_log[int(offset):]

    # Lider
    def promote_observer(self):
        """
        Promove o primeiro observador disponível para votante.
        """
        for member in self.members:
            if member["state"] == "observador":
                member["state"] = "votante"
                # Atualiza o heartbeat
                self.heartbeat_status[member["id"]] = datetime.now()
                member_obj = Pyro5.api.Proxy(member["uri"])
                # Sincroniza o log com o novo votante
                member_obj.reset_log(self.committed_log)
                print(f"Broker {member['id']} promovido a votante.")
                return
        print("Nenhum observador disponível para promover.")


def start_leader():
    """
    Inicia o processo do broker como líder.
    """
    broker = Broker("Lider-Epoca1", "lider")
    broker.heartbeats = {}
    broker.quorum = {}

    daemon = Pyro5.server.Daemon()  # Cria o daemon Pyro5
    ns = Pyro5.api.locate_ns()      # Localiza o serviço de nomes
    uri = daemon.register(broker)  # Registra o broker no daemon
    ns.register("Lider-Epoca1", uri)  # Registra o líder no serviço de nomes

    threading.Thread(target=broker.check_heartbeats).start()

    print("Líder iniciado com URI:", uri)
    daemon.requestLoop()  # Loop para aguardar chamadas remotas


def start_follower(broker_id, state):
    """
    Inicia o processo do broker como votante ou observador.
    :param broker_id: ID do broker.
    :param state: Estado do broker ('votante' ou 'observador').
    """
    broker = Broker(broker_id, state)
    daemon = Pyro5.server.Daemon()
    uri = daemon.register(broker)

    threading.Thread(target=broker.send_heartbeat).start()

    # Conecta ao líder e registra o broker
    lider = Pyro5.api.Proxy("PYRONAME:Lider-Epoca1")
    lider.register_member(uri, broker_id, state)

    print(f"{state.capitalize()} '{broker_id}' registrado no líder.")

    daemon.requestLoop()  # Loop para aguardar chamadas remotas


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Uso: python broker.py <ID> <estado>")
        print("Onde <estado> pode ser: líder, votante ou observador.")
        sys.exit(1)

    broker_id = sys.argv[1]
    state = sys.argv[2].lower()

    if state == "lider":
        start_leader()
    elif state in ("votante", "observador"):
        start_follower(broker_id, state)
    else:
        print("Estado inválido. Use 'lider', 'votante' ou 'observador'.")
        sys.exit(1)
