#![no_std]

//! Client MQTT v3.1.1 minimal, asynchrone, `no_std`.
//!
//! Supporte la QoS 0 (fire-and-forget), l'authentification username/password,
//! le Last Will and Testament, le keep-alive via PINGREQ, ainsi que la
//! souscription (`SUBSCRIBE`) et la réception de messages (`PUBLISH` entrants).
//! Fonctionne avec n'importe quel transport implémentant
//! `embedded_io_async::Read + Write` (TCP, TLS, série...).

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
    /// Le SUBACK reçu est invalide ou mal formé.
    SubackInvalid,
    /// Le broker a refusé la souscription (code de retour SUBACK = 0x80).
    SubscribeFailed,
    /// Paquet entrant inattendu ou mal formé.
    UnexpectedPacket,
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
    packet_id: u16,
}


/// Construit un paquet SUBSCRIBE en QoS 0 — fonction pure, sans I/O, testable directement.
fn build_subscribe_packet(topic: &str, packet_id: u16) -> Result<PacketBuilder, ()> {
    let mut variable_header = PacketBuilder::new();
    variable_header.extend(&packet_id.to_be_bytes())?;

    let mut payload = PacketBuilder::new();
    push_string_field(&mut payload, topic.as_bytes())?;
    payload.push(0x00)?; // QoS 0 demandé

    let mut packet = PacketBuilder::new();
    packet.push(0x82)?; // SUBSCRIBE (flags fixes = 0010)
    encode_remaining_length(variable_header.len + payload.len, &mut packet)?;
    packet.extend(variable_header.as_slice())?;
    packet.extend(payload.as_slice())?;

    Ok(packet)
}

/// Résultat de l'analyse d'un paquet SUBACK.
#[derive(Debug, PartialEq)]
enum SubackResult {
    Accepted,
    Refused,
    Invalid,
}

/// Analyse un SUBACK déjà lu en mémoire — fonction pure, sans I/O.
fn parse_suback(header_byte: u8, body: &[u8]) -> SubackResult {
    if header_byte != 0x90 || body.len() < 3 {
        return SubackResult::Invalid;
    }
    if body[2] == 0x80 {
        return SubackResult::Refused;
    }
    SubackResult::Accepted
}

/// Calcule la longueur du topic et l'offset de début du payload d'un PUBLISH
/// déjà lu en mémoire — fonction pure, sans I/O.
fn publish_layout(header_byte: u8, buf: &[u8]) -> Result<(usize, usize), ()> {
    if buf.len() < 2 {
        return Err(());
    }
    let topic_len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    let qos = (header_byte >> 1) & 0x03;
    let mut payload_start = 2 + topic_len;
    if qos > 0 {
        payload_start += 2; // Packet Identifier (QoS 1/2)
    }
    if payload_start > buf.len() {
        return Err(());
    }
    Ok((topic_len, payload_start))
}
/// Message reçu du broker sur un topic souscrit.
pub struct IncomingMessage<'buf> {
    pub topic: &'buf str,
    pub payload: &'buf [u8],
}



impl<'a, T: Read + Write> MqttClient<'a, T> {
    /// Crée un nouveau client autour d'un transport déjà connecté (socket TCP, etc.)
    pub fn new(transport: &'a mut T) -> Self {
        Self {
            transport,
            packet_id: 1,
        }
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


    

    ///Helper pour lire une "Remaining Length" depuis le réseau (l'inverse de encode_remaining_length)
    async fn read_remaining_length(&mut self) -> Result<usize, MqttError<T::Error>> {
        let mut multiplier: usize = 1;
        let mut value: usize = 0;
        loop {
            let mut byte = [0u8; 1];
            self.transport
                .read_exact(&mut byte)
                .await
                .map_err(|_| MqttError::UnexpectedPacket)?;
            value += ((byte[0] & 0x7F) as usize) * multiplier;
            if byte[0] & 0x80 == 0 {
                break;
            }
            multiplier *= 128;
            if multiplier > 128 * 128 * 128 {
                return Err(MqttError::PacketTooLarge);
            }
        }
        Ok(value)
    }


    /// Souscrit à un topic en QoS 0 et attend la confirmation SUBACK.
    pub async fn subscribe(&mut self, topic: &str) -> Result<(), MqttError<T::Error>> {
        self.packet_id = self.packet_id.wrapping_add(1).max(1);
        let packet =
            build_subscribe_packet(topic, self.packet_id).map_err(|_| MqttError::PacketTooLarge)?;

        self.transport.write_all(packet.as_slice()).await?;

        let mut header = [0u8; 1];
        self.transport
            .read_exact(&mut header)
            .await
            .map_err(|_| MqttError::SubackInvalid)?;

        let remaining_len = self.read_remaining_length().await?;
        if !(3..=8).contains(&remaining_len) {
            return Err(MqttError::SubackInvalid);
        }

        let mut body = [0u8; 8];
        self.transport
            .read_exact(&mut body[..remaining_len])
            .await
            .map_err(|_| MqttError::SubackInvalid)?;

        match parse_suback(header[0], &body[..remaining_len]) {
            SubackResult::Accepted => Ok(()),
            SubackResult::Refused => Err(MqttError::SubscribeFailed),
            SubackResult::Invalid => Err(MqttError::SubackInvalid),
        }
    }

    /// Attend et retourne le prochain message PUBLISH reçu du broker.
    pub async fn receive<'buf>(
        &mut self,
        buf: &'buf mut [u8],
    ) -> Result<IncomingMessage<'buf>, MqttError<T::Error>> {
        loop {
            let mut header = [0u8; 1];
            self.transport
                .read_exact(&mut header)
                .await
                .map_err(|_| MqttError::UnexpectedPacket)?;
            let packet_type = header[0] & 0xF0;
            let remaining_len = self.read_remaining_length().await?;

            if remaining_len > buf.len() {
                return Err(MqttError::PacketTooLarge);
            }

            self.transport
                .read_exact(&mut buf[..remaining_len])
                .await
                .map_err(|_| MqttError::UnexpectedPacket)?;

            if packet_type != 0x30 {
                continue; // Pas un PUBLISH (ex: PINGRESP) : paquet suivant
            }

            let (topic_len, payload_start) = publish_layout(header[0], &buf[..remaining_len])
                .map_err(|_| MqttError::UnexpectedPacket)?;

            let (topic_and_rest, payload_part) = buf[..remaining_len].split_at(payload_start);
            let topic_bytes = &topic_and_rest[2..2 + topic_len];
            let topic =
                core::str::from_utf8(topic_bytes).map_err(|_| MqttError::UnexpectedPacket)?;

            return Ok(IncomingMessage {
                topic,
                payload: payload_part,
            });
        }
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


    #[test]
    fn subscribe_packet_encoding() {
        let packet = build_subscribe_packet("home/clim", 1).unwrap();
        let expected = [
            0x82, 0x0E, // SUBSCRIBE, remaining length = 14
            0x00, 0x01, // Packet Identifier = 1
            0x00, 0x09, b'h', b'o', b'm', b'e', b'/', b'c', b'l', b'i', b'm', // Topic Filter
            0x00, // QoS demandé = 0
        ];
        assert_eq!(packet.as_slice(), &expected);
    }

    #[test]
    fn suback_accepted() {
        let body = [0x00, 0x01, 0x00]; // packet id + return code 0 (QoS0 accordé)
        assert_eq!(parse_suback(0x90, &body), SubackResult::Accepted);
    }

    #[test]
    fn suback_refused() {
        let body = [0x00, 0x01, 0x80];
        assert_eq!(parse_suback(0x90, &body), SubackResult::Refused);
    }

    #[test]
    fn suback_wrong_header_type() {
        let body = [0x00, 0x01, 0x00];
        assert_eq!(parse_suback(0x20, &body), SubackResult::Invalid); // pas un SUBACK
    }

    #[test]
    fn suback_body_too_short() {
        let body = [0x00, 0x01];
        assert_eq!(parse_suback(0x90, &body), SubackResult::Invalid);
    }

    #[test]
    fn publish_layout_qos0() {
        let buf = [0x00, 0x03, b'a', b'/', b'b', b'h', b'i'];
        let (topic_len, payload_start) = publish_layout(0x30, &buf).unwrap();
        assert_eq!(topic_len, 3);
        assert_eq!(payload_start, 5);
        assert_eq!(&buf[payload_start..], b"hi");
    }

    #[test]
    fn publish_layout_qos1_skips_packet_identifier() {
        let buf = [0x00, 0x03, b'a', b'/', b'b', 0x00, 0x2A, b'h', b'i'];
        let (topic_len, payload_start) = publish_layout(0x32, &buf).unwrap();
        assert_eq!(topic_len, 3);
        assert_eq!(payload_start, 7);
        assert_eq!(&buf[payload_start..], b"hi");
    }

    #[test]
    fn publish_layout_rejects_truncated_buffer() {
        let buf = [0x00, 0x05, b'a', b'b']; // annonce topic_len=5 mais buffer trop court
        assert!(publish_layout(0x30, &buf).is_err());
    }





}