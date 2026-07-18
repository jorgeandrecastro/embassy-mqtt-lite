#![no_std]

//! Client MQTT v3.1.1 minimal, asynchrone, `no_std`.
//!
//! Supporte la QoS 0 (fire-and-forget), l'authentification username/password,
//! le Last Will and Testament, et le keep-alive via PINGREQ. Fonctionne avec
//! n'importe quel transport implémentant `embedded_io_async::Read + Write`
//! (TCP, TLS, série...).

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
    /// La réponse au PINGREQ n'est pas un PINGRESP valide.
    PingFailed,
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


/// Ajoute un champ MQTT "UTF-8 string" (préfixé de sa longueur sur 2 octets).
fn push_string_field(builder: &mut PacketBuilder, s: &[u8]) -> Result<(), ()> {
    builder.extend(&(s.len() as u16).to_be_bytes())?;
    builder.extend(s)
}



/// Options de connexion optionnelles : authentification et Last Will.
#[derive(Default)]
pub struct ConnectOptions<'a> {
    pub username: Option<&'a str>,
    pub password: Option<&'a [u8]>,
    pub last_will: Option<LastWill<'a>>,
}

/// Message publié automatiquement par le broker si la connexion est perdue
/// de manière anormale (crash, coupure secteur, timeout keep-alive).
pub struct LastWill<'a> {
    pub topic: &'a str,
    pub message: &'a [u8],
    pub retain: bool,
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

    /// Envoie un paquet CONNECT (Clean Session) et attend le CONNACK.
    /// Version simple, sans authentification ni Last Will.
    pub async fn connect(
        &mut self,
        client_id: &str,
        keep_alive_secs: u16,
    ) -> Result<(), MqttError<T::Error>> {
        self.connect_with_options(client_id, keep_alive_secs, &ConnectOptions::default())
            .await
    }

    /// Envoie un paquet CONNECT avec options (authentification, Last Will) et attend le CONNACK.
    pub async fn connect_with_options(
        &mut self,
        client_id: &str,
        keep_alive_secs: u16,
        options: &ConnectOptions<'_>,
    ) -> Result<(), MqttError<T::Error>> {
        let mut variable_header = PacketBuilder::new();
        variable_header
            .extend(&[0x00, 0x04])
            .map_err(|_| MqttError::PacketTooLarge)?;
        variable_header
            .extend(b"MQTT")
            .map_err(|_| MqttError::PacketTooLarge)?;
        variable_header
            .push(0x04)
            .map_err(|_| MqttError::PacketTooLarge)?;

        let mut flags: u8 = 0x02; // Clean Session
        if let Some(will) = &options.last_will {
            flags |= 0x04;
            if will.retain {
                flags |= 0x20;
            }
        }
        if options.username.is_some() {
            flags |= 0x80;
        }
        if options.password.is_some() {
            flags |= 0x40;
        }
        variable_header
            .push(flags)
            .map_err(|_| MqttError::PacketTooLarge)?;
        variable_header
            .extend(&keep_alive_secs.to_be_bytes())
            .map_err(|_| MqttError::PacketTooLarge)?;

        let mut payload = PacketBuilder::new();
        push_string_field(&mut payload, client_id.as_bytes())
            .map_err(|_| MqttError::PacketTooLarge)?;

        if let Some(will) = &options.last_will {
            push_string_field(&mut payload, will.topic.as_bytes())
                .map_err(|_| MqttError::PacketTooLarge)?;
            push_string_field(&mut payload, will.message)
                .map_err(|_| MqttError::PacketTooLarge)?;
        }
        if let Some(username) = options.username {
            push_string_field(&mut payload, username.as_bytes())
                .map_err(|_| MqttError::PacketTooLarge)?;
        }
        if let Some(password) = options.password {
            push_string_field(&mut payload, password).map_err(|_| MqttError::PacketTooLarge)?;
        }

        let mut packet = PacketBuilder::new();
        packet.push(0x10).map_err(|_| MqttError::PacketTooLarge)?;
        encode_remaining_length(variable_header.len + payload.len, &mut packet)
            .map_err(|_| MqttError::PacketTooLarge)?;
        packet
            .extend(variable_header.as_slice())
            .map_err(|_| MqttError::PacketTooLarge)?;
        packet
            .extend(payload.as_slice())
            .map_err(|_| MqttError::PacketTooLarge)?;

        self.transport.write_all(packet.as_slice()).await?;

        let mut connack = [0u8; 4];
        self.transport
            .read_exact(&mut connack)
            .await
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

    /// Envoie un PINGREQ et attend le PINGRESP correspondant.
    ///
    /// À appeler périodiquement (avant l'expiration du `keep_alive_secs` négocié
    /// lors de `connect`) si aucune autre activité (PUBLISH) n'a lieu sur la
    /// connexion, pour éviter que le broker ne considère le client comme mort.
    pub async fn ping(&mut self) -> Result<(), MqttError<T::Error>> {
        self.transport.write_all(&[0xC0, 0x00]).await?;

        let mut pingresp = [0u8; 2];
        self.transport
            .read_exact(&mut pingresp)
            .await
            .map_err(|_| MqttError::PingFailed)?;

        if pingresp != [0xD0, 0x00] {
            return Err(MqttError::PingFailed);
        }

        Ok(())
    }

    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_length_zero() {
        let mut b = PacketBuilder::new();
        encode_remaining_length(0, &mut b).unwrap();
        assert_eq!(b.as_slice(), &[0x00]);
    }

    #[test]
    fn remaining_length_single_byte_max() {
        let mut b = PacketBuilder::new();
        encode_remaining_length(127, &mut b).unwrap();
        assert_eq!(b.as_slice(), &[0x7F]);
    }

    #[test]
    fn remaining_length_two_bytes() {
        let mut b = PacketBuilder::new();
        encode_remaining_length(200, &mut b).unwrap();
        assert_eq!(b.as_slice(), &[0xC8, 0x01]);
    }

    #[test]
    fn remaining_length_three_bytes() {
        let mut b = PacketBuilder::new();
        encode_remaining_length(16384, &mut b).unwrap();
        assert_eq!(b.as_slice(), &[0x80, 0x80, 0x01]);
    }

    #[test]
    fn string_field_encoding() {
        let mut b = PacketBuilder::new();
        push_string_field(&mut b, b"MQTT").unwrap();
        assert_eq!(b.as_slice(), &[0x00, 0x04, b'M', b'Q', b'T', b'T']);
    }

    #[test]
    fn packet_builder_rejects_overflow() {
        let mut b = PacketBuilder::new();
        let big = [0u8; MAX_PACKET_SIZE + 1];
        assert!(b.extend(&big).is_err());
    }
}