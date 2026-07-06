# embassy-mqtt-lite

[![Crates.io](https://img.shields.io/crates/v/embassy-mqtt-lite.svg)](https://crates.io/crates/embassy-mqtt-lite)
[![Documentation](https://docs.rs/embassy-mqtt-lite/badge.svg)](https://docs.rs/embassy-mqtt-lite)
[![License](https://img.shields.io/badge/license-GPL--2.0--or--later-blue.svg)](LICENSE)
[![no_std](https://img.shields.io/badge/no__std-yes-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)
[![Downloads](https://img.shields.io/crates/d/embassy-mqtt-lite.svg)](https://crates.io/crates/embassy-mqtt-lite)

Un client **MQTT v3.1.1** minimal, asynchrone et `no_std`, agnostique du transport.

Conçu pour les projets embarqués basés sur [Embassy](https://embassy.dev/) (ESP32, RP2040, STM32, nRF...), sans dépendre d'un allocateur (`alloc`) et sans imposer de pile réseau spécifique : n'importe quel transport implémentant `embedded_io_async::Read + Write` (socket TCP, TLS, série...) fait l'affaire.

## ✨ Fonctionnalités

- ✅ Connexion MQTT 3.1.1 (`CONNECT` / `CONNACK`), Clean Session
- ✅ Publication en **QoS 0** (fire-and-forget)
- ✅ `no_std`, zéro allocation dynamique (buffers de taille fixe sur la pile)
- ✅ Agnostique du transport : TCP, TLS, ou tout autre flux `embedded-io-async`
- ✅ Aucune dépendance lourde une seule dépendance : `embedded-io-async`
- ✅ Compatible avec n'importe quelle pile réseau Embassy (`embassy-net`, etc.)

## 🚧 Limitations actuelles

- Seule la **QoS 0** est supportée (pas de QoS 1/2, pas d'accusés applicatifs)
- Pas de souscription (`SUBSCRIBE`) publication uniquement pour l'instant
- Pas d'authentification (user/password) ni de TLS intégré (le TLS peut être géré en amont via le transport fourni)
- Pas de gestion automatique du `PINGREQ`/keep-alive  à gérer côté application si nécessaire

Ces limitations correspondent à un usage simple de reporting/télémétrie. Des contributions pour étendre les fonctionnalités sont bienvenues !

## 📦 Installation

```toml
[dependencies]
embassy-mqtt-lite = "0.1"
```

## 🚀 Exemple d'utilisation

```rust
use embassy_mqtt_lite::MqttClient;
use embassy_net::tcp::TcpSocket;

async fn publish_example(stack: embassy_net::Stack<'static>) {
    let mut rx_buffer = [0u8; 512];
    let mut tx_buffer = [0u8; 512];
    let mut socket = TcpSocket::new(stack, &mut rx_buffer, &mut tx_buffer);

    let remote_endpoint = (embassy_net::IpAddress::v4(192, 168, 1, 84), 1883);
    socket.connect(remote_endpoint).await.unwrap();

    let mut client = MqttClient::new(&mut socket);

    client.connect("mon-client-esp32", 60).await.unwrap();

    client
        .publish("maison/salon/temperature", b"21.5")
        .await
        .unwrap();
}
```

## 🔧 Comment ça marche

La crate construit les paquets MQTT bruts (`CONNECT`, `PUBLISH`) dans un buffer de taille fixe (`MAX_PACKET_SIZE`, 256 octets par défaut, ajustable dans le code source) puis les envoie via le trait `embedded_io_async::Write` du transport fourni. La réponse `CONNACK` est lue et validée via `embedded_io_async::Read`.

Aucune hypothèse n'est faite sur la pile réseau : que ce soit `embassy-net`, un autre stack TCP/IP, ou même un flux série faisant office de passerelle, tant que le type implémente `Read + Write` de `embedded-io-async`, ça fonctionne.

## 🎯 Pourquoi cette crate ?

Les clients MQTT `no_std` existants pour l'écosystème Rust embarqué imposent souvent des dépendances lourdes, des générations de types complexes, ou des versions de traits (`embedded-io-async`) qui entrent en conflit avec le reste de la stack Embassy/`esp-hal`. `embassy-mqtt-lite` vise la simplicité : une implémentation directe et lisible du strict nécessaire pour publier des données de capteurs vers un broker MQTT, sans négociation de version douloureuse.

## 🤝 Contribuer

Les issues et pull requests sont les bienvenues sur le [dépôt GitHub](https://github.com/jorgeandrecastro/embassy-mqtt-lite).

## 📄 Licence

Distribué sous licence **GPL-2.0-or-later**. Voir le fichier [LICENSE](LICENSE) pour plus de détails.

## 👤 Auteur

**Jorge Andre Castro**  [georgeandrec@gmail.com](mailto:georgeandrec@gmail.com)