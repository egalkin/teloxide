use super::{serializer::Serializer, Storage};
use bb8::RunError;
use futures::future::BoxFuture;
use serde::{de::DeserializeOwned, Serialize};
use skytable::{error::Error, pool, pool::ConnectionMgrTcp, query, Config};
use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    sync::Arc,
};
use teloxide_core::types::ChatId;
use thiserror::Error;

type SkytablePool = bb8::Pool<ConnectionMgrTcp>;

/// See [Skytable erros](https://docs.skytable.io/protocol/errors/)
const ROW_ALREADY_EXISTS_CODE: u16 = 108;
const ROW_NOT_FOUND_CODE: u16 = 111;

#[derive(Debug, Error)]
pub enum SkytableStorageError<SE>
where
    SE: Debug + Display,
{
    #[error("dialogue serialization error: {0}")]
    SerdeError(SE),

    #[error("run error: {0}")]
    RunError(#[from] RunError<Error>),

    #[error("skytable error: {0}")]
    SkytableError(#[from] Error),

    /// Returned from [`SkytableStorage::remove_dialogue`].
    #[error("row not found")]
    DialogueNotFound,
}

/// A persistent dialogue storage based on [Skytable](https://skytable.io/)
pub struct SkytableStorage<S> {
    pool: SkytablePool,
    serializer: S,
}

impl<S> SkytableStorage<S> {
    /// Opens a connection pool to the [Skytable](https://skytable.io/) database and creates the model
    /// for storing dialogues.
    ///
    /// Parameters:
    /// - skytable_host: host of the skytable database
    /// - skytable_port: port of the skytable database
    /// - skytable_user: user of the skytable database
    /// - skytable_password: password for the passed skytable user
    /// - max_connections: number of connections in creating connection pool.
    /// - serializer: what [`Serializer`] will be used to encode the dialogue
    ///   data. Available ones are: [`Json`], [`Bincode`], [`Cbor`]
    ///
    /// [`Json`]: crate::dispatching::dialogue::serializer::Json
    /// [`Bincode`]: crate::dispatching::dialogue::serializer::Bincode
    /// [`Cbor`]: crate::dispatching::dialogue::serializer::Cbor
    pub async fn open(
        skytable_host: &str,
        skytable_port: u16,
        skytable_user: &str,
        skytable_password: &str,
        max_connections: u32,
        serializer: S,
    ) -> Result<Arc<Self>, SkytableStorageError<Infallible>> {
        let config = Config::new(skytable_host, skytable_port, skytable_user, skytable_password);
        let mut conn = config.connect()?;
        conn.query_parse::<bool>(&query!("create space if not exists teloxide"))?;
        conn.query_parse::<bool>(&query!(
            "create model if not exists teloxide.dialogues(chat_id: uint64, dialogue: binary)",
        ))?;

        let pool = pool::get_async(max_connections, config).await?;
        Ok(Arc::new(Self { pool, serializer }))
    }
}

impl<S, D> Storage<D> for SkytableStorage<S>
where
    S: Send + Sync + Serializer<D> + 'static,
    D: Send + Serialize + DeserializeOwned + 'static,
    <S as Serializer<D>>::Error: Debug + Display,
{
    type Error = SkytableStorageError<<S as Serializer<D>>::Error>;

    fn remove_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async move {
            let mut conn = self.pool.get().await?;

            let delete_result = conn
                .query_parse::<()>(&query!(
                    "delete from teloxide.dialogues where chat_id = ?",
                    chat_id as u64
                ))
                .await;

            match delete_result {
                Ok(_) => Ok(()),
                Err(skytable_err) => match skytable_err {
                    Error::ServerError(ROW_NOT_FOUND_CODE) => {
                        Err(SkytableStorageError::DialogueNotFound)
                    }
                    _ => Err(SkytableStorageError::SkytableError(skytable_err)),
                },
            }
        })
    }

    fn update_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
        dialogue: D,
    ) -> BoxFuture<'static, Result<(), Self::Error>> {
        Box::pin(async move {
            let d =
                self.serializer.serialize(&dialogue).map_err(SkytableStorageError::SerdeError)?;
            let mut conn = self.pool.get().await?;

            let insert_result = conn
                .query_parse::<()>(&query!(
                    "insert into teloxide.dialogues(?, ?)",
                    chat_id as u64,
                    &d
                ))
                .await;

            match insert_result {
                Ok(_) => Ok(()),
                Err(skytable_err) => match skytable_err {
                    Error::ServerError(ROW_ALREADY_EXISTS_CODE) => {
                        let update_result = conn
                            .query_parse::<()>(&query!(
                                "update teloxide.dialogues set dialogue = ? where chat_id = ?",
                                &d,
                                chat_id as u64
                            ))
                            .await;

                        match update_result {
                            Ok(_) => Ok(()),
                            _ => Err(SkytableStorageError::SkytableError(skytable_err)),
                        }
                    }
                    _ => Err(SkytableStorageError::SkytableError(skytable_err)),
                },
            }
        })
    }

    fn get_dialogue(
        self: Arc<Self>,
        ChatId(chat_id): ChatId,
    ) -> BoxFuture<'static, Result<Option<D>, Self::Error>> {
        Box::pin(async move {
            let mut conn = self.pool.get().await?;

            let dialogue: Option<Vec<u8>> = match conn
                .query_parse::<(Vec<u8>,)>(&query!(
                    "select dialogue from teloxide.dialogues where chat_id = ?",
                    chat_id as u64
                ))
                .await
            {
                Ok(val) => Some(val.0),
                _ => None,
            };

            dialogue
                .map(|d| self.serializer.deserialize(&d).map_err(SkytableStorageError::SerdeError))
                .transpose()
        })
    }
}
