#![no_std]

//! Client MQTT v3.1.1 minimal, asynchrone, `no_std`.
//!
//! Ne supporte que la QoS 0 (fire-and-forget) et la connexion sans
//! authentification/TLS pour l'instant. Fonctionne avec n'importe quel
//! transport implémentant `embedded_io_async::Read + Write` (TCP, TLS, série...).

use embedded_io_async::{Read, Write};

/// Taille maximale (en octets) d'un paquet CONNECT ou PUBLISH construit par cette crate.
/// Augmente cette constante si tu as des topics/payloads plus longs.
pub const MAX_PACKET_SIZE: usize = 256;

/// Erreurs possibles lors de l'utilisation du client MQTT.
#[derive(Debug)]
pub enum MqttError<E> {
    /// Erreur de transport (TCP, TLS, etc.)
    Io(E),
    /// Le paquet à construire dépasse `MAX_PACKET_SIZE`.
    PacketTooLarge,
    /// Le broker n'a pas répondu par un CONNACK valide.
    ConnackInvalid,
    /// Le broker a explicitement refusé la connexion (code de retour CONNACK non nul).
    ConnectionRefused(u8),
}

impl<E> From<E> for MqttError<E> {
    fn from(e: E) -> Self {
        MqttError::Io(e)
    }
}

/// Buffer de construction de paquet, taille fixe, sans allocation dynamique.
struct PacketBuilder {
    buf: [u8; MAX_PACKET_SIZE],
    len: usize,
}

impl PacketBuilder {
    fn new() -> Self {
        Self {
            buf: [0u8; MAX_PACKET_SIZE],
            len: 0,
        }
    }

    fn push(&mut self, byte: u8) -> Result<(), ()> {
        if self.len >= MAX_PACKET_SIZE {
            return Err(());
        }
        self.buf[self.len] = byte;
        self.len += 1;
        Ok(())
    }

    fn extend(&mut self, bytes: &[u8]) -> Result<(), ()> {
        if self.len + bytes.len() > MAX_PACKET_SIZE {
            return Err(());
        }
        self.buf[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
        Ok(())
    }

    fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// Encode la "Remaining Length" au format variable-length du protocole MQTT.
fn encode_remaining_length(mut len: usize, out: &mut PacketBuilder) -> Result<(), ()> {
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte)?;
        if len == 0 {
            break;
        }
    }
    Ok(())
}

/// Client MQTT minimal opérant sur un transport `Read + Write` fourni par l'appelant.
pub struct MqttClient<'a, T: Read + Write> {
    transport: &'a mut T,
}

impl<'a, T: Read + Write> MqttClient<'a, T> {
    /// Crée un nouveau client autour d'un transport déjà connecté (socket TCP, etc.)
    pub fn new(transport: &'a mut T) -> Self {
        Self { transport }
    }

    /// Envoie un paquet CONNECT (Clean Session, sans authentification) et attend le CONNACK.
    pub async fn connect(
        &mut self,
        client_id: &str,
        keep_alive_secs: u16,
    ) -> Result<(), MqttError<T::Error>> {
        let mut variable_header = PacketBuilder::new();
        variable_header
            .extend(&[0x00, 0x04])
            .map_err(|_| MqttError::PacketTooLarge)?;
        variable_header
            .extend(b"MQTT")
            .map_err(|_| MqttError::PacketTooLarge)?;
        variable_header
            .push(0x04) // Protocol Level : MQTT 3.1.1
            .map_err(|_| MqttError::PacketTooLarge)?;
        variable_header
            .push(0x02) // Connect Flags : Clean Session
            .map_err(|_| MqttError::PacketTooLarge)?;
        variable_header
            .extend(&keep_alive_secs.to_be_bytes())
            .map_err(|_| MqttError::PacketTooLarge)?;

        let id_bytes = client_id.as_bytes();
        variable_header
            .extend(&(id_bytes.len() as u16).to_be_bytes())
            .map_err(|_| MqttError::PacketTooLarge)?;
        variable_header
            .extend(id_bytes)
            .map_err(|_| MqttError::PacketTooLarge)?;

        let mut packet = PacketBuilder::new();
        packet.push(0x10).map_err(|_| MqttError::PacketTooLarge)?; // Type : CONNECT
        encode_remaining_length(variable_header.len, &mut packet)
            .map_err(|_| MqttError::PacketTooLarge)?;
        packet
            .extend(variable_header.as_slice())
            .map_err(|_| MqttError::PacketTooLarge)?;

        self.transport.write_all(packet.as_slice()).await?;

        // Le CONNACK fait toujours 4 octets : [0x20, 0x02, session_present, return_code]
        let mut connack = [0u8; 4];
        self.transport.read_exact(&mut connack).await
            .map_err(|_| MqttError::ConnackInvalid)?;

        if connack[0] != 0x20 || connack[1] != 0x02 {
            return Err(MqttError::ConnackInvalid);
        }
        if connack[3] != 0x00 {
            return Err(MqttError::ConnectionRefused(connack[3]));
        }

        Ok(())
    }

    /// Publie un message en QoS 0 (fire-and-forget) sur le topic donné.
    pub async fn publish(&mut self, topic: &str, payload: &[u8]) -> Result<(), MqttError<T::Error>> {
        let mut variable_header = PacketBuilder::new();
        let topic_bytes = topic.as_bytes();
        variable_header
            .extend(&(topic_bytes.len() as u16).to_be_bytes())
            .map_err(|_| MqttError::PacketTooLarge)?;
        variable_header
            .extend(topic_bytes)
            .map_err(|_| MqttError::PacketTooLarge)?;
        // QoS 0 : pas de Packet Identifier

        let mut packet = PacketBuilder::new();
        packet.push(0x30).map_err(|_| MqttError::PacketTooLarge)?; // PUBLISH, QoS0, DUP=0, RETAIN=0
        encode_remaining_length(variable_header.len + payload.len(), &mut packet)
            .map_err(|_| MqttError::PacketTooLarge)?;
        packet
            .extend(variable_header.as_slice())
            .map_err(|_| MqttError::PacketTooLarge)?;
        packet.extend(payload).map_err(|_| MqttError::PacketTooLarge)?;

        self.transport.write_all(packet.as_slice()).await?;
        Ok(())
    }
}