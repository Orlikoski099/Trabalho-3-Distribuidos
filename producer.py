import Pyro5.api


def main():
    líder = Pyro5.api.Proxy("PYRONAME:Lider-Epoca1")
    while True:
        data = input("Digite o dado para publicar (ou 'sair' para terminar): ")
        if data.lower() == "sair":
            break
        líder.append_log(data)
        print("Dado publicado.")


if __name__ == "__main__":
    main()
