#[allow(unused)]
use tracing::{trace, debug, info, warn, error, instrument, Level};
use async_trait::async_trait;
use uuid::Uuid;

use std::{net::SocketAddr, str::FromStr};
use std::sync::Arc;
use std::path::Path;
use std::collections::HashMap;

use russh::{
    Channel, ChannelId,
    server::{Server, Msg, Handler, Auth, Session},
};
use russh_sftp::protocol::{
    StatusCode,
    Status,
    FileAttributes, OpenFlags,
    Handle as SFTPHandle, Name as SFTPName, File as SFTPFile, Attrs as SFTPAttrs, Data as SFTPData,
};
use ssh_key::{public::PublicKey, private::PrivateKey};

use super::{tys::{DirectoryID, Error as NodeError}, FrontNode};
use super::config;

#[derive(Debug)]
#[allow(unused)]
pub enum SSHError {
    SSHError(russh::Error),
    IO(std::io::Error),
    CouldNotParseAddr,

    ReadPublicKeyError(ssh_key::Error),
    ReadPrivateKeyError(ssh_key::Error),
}

impl From<russh::Error> for SSHError {
    fn from(v: russh::Error) -> Self {
        Self::SSHError(v)
    }
}

impl From<std::io::Error> for SSHError {
    fn from(v: std::io::Error) -> Self {
        Self::IO(v)
    }
}

type SSHResult<T> = std::result::Result<T, SSHError>;

struct SSHServer {
    node: Arc<FrontNode>,
}

#[async_trait]
impl Server for SSHServer {
    type Handler = SSHSession;

    fn new_client(&mut self, client_addr: Option<SocketAddr>) -> SSHSession {
        debug!(?client_addr, "new SSH connection");
        SSHSession {
            client_addr,
            user: None,
            node: self.node.clone(),
            open_channels: HashMap::new(),
        }
    }
}

struct SSHSession {
    client_addr: Option<SocketAddr>,
    user: Option<String>,
    node: Arc<FrontNode>,
    open_channels: HashMap<ChannelId, Channel<Msg>>,
}

impl std::fmt::Debug for SSHSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SSHSession")?;
        if let Some(ref user) = self.user {
            write!(f, " for {user}")?;
        }
        if let Some(client_addr) = self.client_addr {
            write!(f, "@{}", client_addr)?;
        } else {
            write!(f, "@unknown")?;
        }
        Ok(())
    }
}


#[async_trait]
impl Handler for SSHSession {
    type Error = SSHError;

    // TODO: implement close

    #[instrument(level = "debug", skip(_pubkey))]
    async fn auth_publickey(&mut self, user: &str, _pubkey: &ssh_key::public::PublicKey)
        -> SSHResult<Auth>
    {
        // TODO: Verify the key, somehow
        self.user = Some(user.to_owned());
        debug!("user authing");
        Ok(Auth::Accept)
    }

    #[instrument(level = "trace", skip(channel, _session))]
    async fn channel_open_session(&mut self, channel: Channel<Msg>, _session: &mut Session) -> SSHResult<bool> {
        let id = channel.id();
        self.open_channels.insert(id, channel);

        Ok(true) // grant open request
    }

    #[instrument(level = "debug", skip(id, session))]
    async fn subsystem_request(&mut self, id: ChannelId, name: &str, session: &mut Session) -> SSHResult<()> {
        let Some(user) = self.user.clone() else {
            session.channel_failure(id)?;
            error!(?id, "User authed without name being recorded.");
            return Err(russh::Error::RequestDenied.into());
        };

        if name == "sftp" {
            debug!(?id, "requesting sftp subsystem");
            let channel = self.open_channels.remove(&id).unwrap(); // russh guarantees(?) this channel_id is active

            let sftp_connection = SFTPConnection::new(self.node.clone(), user, self.client_addr);

            russh_sftp::server::run(
                channel.into_stream(),
                sftp_connection,
            ).await;

            session.channel_success(id)?;
        } else {
            session.channel_failure(id)?;
            debug!(?id, name, "requesting unknown subsystem");
        }
        Ok(())
    }
}

enum Handle {
    File(Uuid),
    Directory(DirectoryID),
}

impl ToString for Handle {
    fn to_string(&self) -> String {
        match self {
            Handle::File(uuid) => {
                let uuid_str = uuid.hyphenated()
                    .encode_lower(&mut Uuid::encode_buffer())
                    .to_string();
                format!("f:{uuid_str}")
            }
            Handle::Directory(id) => format!("d:{}", id.0),
        }
    }
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Handle::File(uuid) => {
                let uuid_str = uuid.hyphenated()
                    .encode_lower(&mut Uuid::encode_buffer())
                    .to_string();
                write!(f, "Handle::File({uuid_str})")
            }
            Handle::Directory(id) => write!(f, "Handle::Directory({})", id.0),
        }
    }
}

impl FromStr for Handle {
    type Err = StatusCode;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (prefix, suffix) = s.split_at_checked(2).ok_or(StatusCode::BadMessage)?;
        match prefix {
            "f:" => {
                let uuid = Uuid::try_parse(suffix).map_err(|_| StatusCode::BadMessage)?;
                Ok(Handle::File(uuid))
            }
            "d:" => {
                let dir_id: i64 = suffix.parse().map_err(|_| StatusCode::BadMessage)?;
                Ok(Handle::Directory(DirectoryID(dir_id)))
            }
            _ => Err(StatusCode::BadMessage),
        }
    }
}

#[derive(PartialEq)]
enum DirectoryStatus {
    Unread, Read
}

struct FileStatus {
    append: bool,
}

struct SFTPConnection {
    node: Arc<FrontNode>,

    #[allow(unused)]
    client_version: Option<u32>,
    #[allow(unused)]
    client_extensions: HashMap<String, String>,

    user: String,
    remote_addr: Option<SocketAddr>,

    directory_status: HashMap<DirectoryID, DirectoryStatus>,
    file_status: HashMap<Uuid, FileStatus>,
}

impl SFTPConnection {
    fn new(
        node: Arc<FrontNode>,
        user: String, remote_addr: Option<SocketAddr>,
    ) -> Self {
        Self {
            node,
            client_version: None,
            client_extensions: HashMap::new(),
            user,
            remote_addr,
            directory_status: HashMap::new(),
            file_status: HashMap::new(),
        }
    }
}

impl std::fmt::Debug for SFTPConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SFTPConnection for {}", self.user)?;
        Ok(())
    }
}

type SFTPResult<T> = std::result::Result<T, StatusCode>;

fn status_ok(id: u32) -> Status {
    Status {
        id,
        status_code: StatusCode::Ok,
        error_message: "ok".to_string(),
        language_tag: "en-US".to_string(),
    }
}

const ATTR_PERMISSION_DIRECTORY: u32 = 0o0040000;
const ATTR_PERMISSION_FILE: u32 = 0o0100000;

impl SFTPConnection {
    fn normalize_path(&self, path: String) -> SFTPResult<String> {
        // remove trailing slashes (some clients seem to add them)
        let path = if path.ends_with("/") {
            &path[..path.len()-1]
        } else {
            &path
        };

        // remove all instances of x/../
        let mut parts = Vec::new();
        for part in path.split('/') {
            if part == ".." {
                if parts.len() == 0 {
                    return Err(StatusCode::BadMessage);
                }
                parts.pop();
            } else if part == "." {
                continue;
            } else {
                parts.push(part);
            }
        }
        let mut canon = String::new();
        for (part, is_first) in parts.into_iter().zip(std::iter::once(true).chain(std::iter::repeat(false))) {
            if !is_first {
                canon.push('/');
            }
            canon.push_str(part);
        }

        Ok(canon)
    }

    // for relative paths (not starting with /), return user home directory id
    // for absolute paths (starting with /), remove the / and give the root node (None)
    async fn absolutize_path(&self, path: String) -> SFTPResult<(Option<DirectoryID>, String)> {
        let mut path = self.normalize_path(path)?;

        // un-relative the path
        if path.starts_with('/') {
            Ok((None, path.split_off(1)))
        } else {
            let user_root = match self.node.home_for_user(&self.user).await {
                Ok(dir) => dir,
                Err(e) => {
                    error!(self.user, ?e, "Failed to find home directory for logged-in user");
                    return Err(StatusCode::Failure);
                }
            };
            Ok((Some(user_root), path))
        }
    }

    async fn handle_from_path(&self, path: String) -> Result<Handle, StatusCode> {
        let (base, path) = self.absolutize_path(path).await?;

        // prioritize if there's a directory with this path
        match self.node.directory_id_for_path(&path, base).await {
            Ok(dir) => return Ok(Handle::Directory(dir)),
            Err(NodeError::NoSuchDirectory { .. } ) => {}
            Err(e) => {
                error!(?e, ?path, "Could not fetch directory");
            }
        };

        // otherwise, check for a file
        match self.node.file_uuid_for_path(&path, base).await {
            Ok(uuid) => return Ok(Handle::File(uuid)),
            Err(NodeError::NoSuchDirectory { .. } | NodeError::NoSuchFile) => {}
            Err(e) => {
                error!(?e, ?path, "Could not fetch file");
            }
        };

        Err(StatusCode::NoSuchFile)
    }

    async fn attrs_for_handle(&self, handle: Handle) -> Result<FileAttributes, StatusCode> {
        // TODO: set permissions once we have added those to the database schema
        match handle {
            Handle::File(_) => Ok(FileAttributes {
                permissions: Some(0o777 | ATTR_PERMISSION_FILE),
                ..Default::default()
            }),
            Handle::Directory(_) => Ok(FileAttributes {
                permissions: Some(0o777 | ATTR_PERMISSION_DIRECTORY),
                ..Default::default()
            }),
        }
    }

    // tail-called by stat, lstat and fstat
    #[instrument(level = "debug", skip(id))]
    async fn handle_stat(&mut self, id: u32, handle: Handle) -> SFTPResult<SFTPAttrs> {
        Ok(SFTPAttrs {
            id,
            attrs: self.attrs_for_handle(handle).await?,
        })
    }
}

#[async_trait]
impl russh_sftp::server::Handler for SFTPConnection {
    type Error = StatusCode;

    fn unimplemented(&self) -> StatusCode {
        StatusCode::Failure
    }

    #[instrument(level = "debug", skip(extensions))]
    async fn init(&mut self, client_version: u32, extensions: HashMap<String, String>)
        -> SFTPResult<russh_sftp::protocol::Version>
    {
        self.client_version = Some(client_version);
        self.client_extensions = extensions;
        Ok(russh_sftp::protocol::Version::new())
    }

    #[instrument(level = "debug", skip(id))]
    async fn realpath(&mut self, id: u32, path: String) -> SFTPResult<SFTPName> {
        let canon = self.normalize_path(path)?;

        Ok(SFTPName {
            id,
            files: vec![SFTPFile::dummy(canon)],
        })
    }

    // directory listings happen by first calling opendir, then repeatedly calling readdir
    // until it returns an EOF status code
    // therefore, we need to keep track of if the directory has been readdir'd yet
    #[instrument(level = "debug", skip(id))]
    async fn opendir(&mut self, id: u32, path: String) -> SFTPResult<SFTPHandle> {
        let Handle::Directory(dir_id) = self.handle_from_path(path).await? else {
            return Err(StatusCode::NoSuchFile);
        };

        self.directory_status.insert(dir_id, DirectoryStatus::Unread);

        Ok(SFTPHandle {
            id,
            handle: Handle::Directory(dir_id).to_string(),
        })
    }

    #[instrument(level = "debug", skip(id))]
    async fn readdir(&mut self, id: u32, handle: String) -> SFTPResult<SFTPName> {
        let Handle::Directory(dir) = handle.parse()? else {
            return Err(StatusCode::BadMessage);
        };
        let Some(status) = self.directory_status.get_mut(&dir) else {
            warn!("Listing unopened directory");
            return Err(StatusCode::BadMessage);
        };

        if *status == DirectoryStatus::Read {
            return Err(StatusCode::Eof);
        }

        *status = DirectoryStatus::Read;

        let listing = match self.node.list_directory(dir).await {
            Ok(listing) => listing,
            Err(e) => {
                error!(?e, "error listing directory");
                return Err(StatusCode::Failure);
            }
        };

        let mut files = Vec::new();
        for (uuid, name) in listing.file_uuids_and_names {
            let attrs = self.attrs_for_handle(Handle::File(uuid)).await?;

            files.push(SFTPFile {
                filename: name.clone(),
                // TODO: this should take the form of an ls listing
                longname: format!("-rwxr-xr-x   1 mjos     staff      348911 Mar 25 14:29 t-filexfer"),
                attrs,
            });
        }
        for (dir_id, name) in listing.directory_ids_and_names {
            let attrs = self.attrs_for_handle(Handle::Directory(dir_id)).await?;

            files.push(SFTPFile {
                filename: name.clone(),
                // TODO: this should take the form of an ls listing
                longname: format!("-rwxr-xr-x   1 mjos     staff      348911 Mar 25 14:29 t-filexfer"),
                attrs,
            });
        }

        Ok(SFTPName {
            id,
            files,
        })
    }

    #[instrument(level = "debug", skip(id, _attrs))]
    async fn open(&mut self, id: u32, path: String, open_flags: OpenFlags, _attrs: FileAttributes)
        -> SFTPResult<SFTPHandle>
    {
        let existing_uuid: Option<Uuid> = match self.handle_from_path(path).await {
            Ok(Handle::File(uuid)) => Some(uuid),
            Ok(Handle::Directory(_)) | Err(StatusCode::NoSuchFile) => None,
            _ => return Err(StatusCode::Failure),
        };

        let uuid = {
            if open_flags.contains(OpenFlags::CREATE) {
                if open_flags.contains(OpenFlags::EXCLUDE) && existing_uuid.is_some() {
                    debug!("Tried to open an existing file with CREATE + EXCLUDE");
                    return Err(StatusCode::Failure);
                }
                if let Some(uuid) = existing_uuid {
                    uuid
                } else {
                    // TODO: we kinda wanna rework the FrontNode API for creating/writing to/reading from files
                    // currently, file_uuid_for_path gives an UUID for an existing file
                    // get_file takes the UUID and returns the content (should probably be called read_file)
                    // upload_file takes a directory ID, a name and data, and creates a file with that name and writes the data to it
                    error!("Creating files not yet supported");
                    return Err(StatusCode::Failure);
                }
            } else {
                if let Some(uuid) = existing_uuid {
                    uuid
                } else {
                    debug!("Tried opening non-existant file");
                    return Err(StatusCode::Failure);
                }
            }
        };

        // TODO: do we need to track if we open the file in read mode?
        // i think it should be standard-compliant to allow writing to files opened ind read mode and vice-versa
        let status = FileStatus {
            append: open_flags.contains(OpenFlags::APPEND),
        };

        self.file_status.insert(uuid, status);

        Ok(SFTPHandle {
            id,
            handle: Handle::File(uuid).to_string(),
        })
    }

    #[instrument(level = "debug", skip(id))]
    async fn read(&mut self, id: u32, handle: String, offset: u64, len: u32) -> SFTPResult<SFTPData> {
        let Handle::File(uuid) = handle.parse()? else {
            return Err(StatusCode::BadMessage);
        };

        let (mut data, _info) = match self.node.get_file(uuid).await {
            Ok(x) => x,
            Err(NodeError::NotConnectedToNode) => {
                warn!(%uuid, "Could not read file; node not connected");
                return Err(StatusCode::Failure);
            }
            Err(e) => {
                error!(%uuid, ?e, "Could not read file");
                return Err(StatusCode::Failure);
            }
        };

        if offset as usize >= data.len() {
            return Err(StatusCode::Eof);
        }

        let mut data = data.split_off(offset as usize);
        if len as usize <= data.len() {
            data.truncate(len as usize);
        }

        Ok(SFTPData {
            id,
            data,
        })
    }


    #[instrument(level = "debug", skip(id))]
    async fn stat(&mut self, id: u32, path: String) -> SFTPResult<SFTPAttrs> {
        let handle = self.handle_from_path(path).await?;
        self.handle_stat(id, handle).await
    }

    #[instrument(level = "debug", skip(id))]
    async fn lstat(&mut self, id: u32, path: String) -> SFTPResult<SFTPAttrs> {
        let handle = self.handle_from_path(path).await?;
        self.handle_stat(id, handle).await
    }

    #[instrument(level = "debug", skip(id))]
    async fn fstat(&mut self, id: u32, handle: String) -> SFTPResult<SFTPAttrs> {
        let handle: Handle = handle.parse()?;
        self.handle_stat(id, handle).await
    }

    #[instrument(level = "debug", skip(id))]
    async fn close(&mut self, id: u32, handle: String) -> SFTPResult<Status> {
        let handle: Handle = handle.parse()?;
        match handle {
            Handle::File(ref uuid) => {
                if self.file_status.remove(uuid).is_none() {
                    warn!(?handle, "Tried to close non-opened handle");
                    return Err(StatusCode::Failure);
                }
            }
            Handle::Directory(ref dir_id) => {
                if self.directory_status.remove(dir_id).is_none() {
                    warn!(?handle, "Tried to close non-opened directory");
                    return Err(StatusCode::Failure);
                }
            }
        }
        return Ok(status_ok(id));
    }

}

// TODO: we should read these files asyncly
fn read_server_keypair(
    cfg: &config::SFTPServerOptions,
) -> SSHResult<(PublicKey, PrivateKey)> {
    let public = PublicKey::read_openssh_file(Path::new(&cfg.public_key))
        .map_err(SSHError::ReadPublicKeyError)?;
    let private = PrivateKey::read_openssh_file(Path::new(&cfg.private_key))
        .map_err(SSHError::ReadPrivateKeyError)?;

    // TODO: Verify private and public match
    Ok((public, private))
}

// Handles errors by printing to STDOUT and (probably) returning
#[instrument(skip(cfg, node))]
#[allow(unused_variables)]
pub async fn launch_sftp_server(
    cfg: &config::SFTPServerOptions,
    node: Arc<FrontNode>,
) {
    let (_public_key, private_key) = match read_server_keypair(cfg) {
        Ok(x) => x,
        Err(SSHError::ReadPublicKeyError(e)) => {
            error!(?e, "Could not read public key");
            return;
        }
        Err(SSHError::ReadPrivateKeyError(e)) => {
            error!(?e, "Could not read private key");
            return;
        }
        _ => unreachable!(),
    };

    use std::time::Duration;
    let ssh_config = russh::server::Config {
        auth_banner: Some("welcome to bnuystore!!\n"),
        auth_rejection_time: Duration::from_secs(3),
        auth_rejection_time_initial: Some(Duration::from_secs(0)),
        keys: vec![private_key],
        ..Default::default()
    };

    let Ok(addr) =  cfg.listen_addr.parse::<SocketAddr>() else {
        error!("Could not parse SFTP address {}. Format must be IP:PORT", cfg.listen_addr);
        return;
    };

    info!(%addr, "Launching SSH server");
    let mut server = SSHServer { node };
    match server.run_on_address(
        Arc::new(ssh_config),
        addr,
    ).await {
        Ok(_) => {},
        Err(e) => {
            error!(?e, "Failed to run SSH server");
        }
    }
}
