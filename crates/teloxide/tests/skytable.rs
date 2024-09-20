use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use teloxide::{
    dispatching::dialogue::{Serializer, SkytableStorage, SkytableStorageError, Storage},
    types::ChatId,
};

#[tokio::test]
#[cfg_attr(not(CI_SKYTABLE), ignore)]
async fn test_skytable_json() {
    let storage = SkytableStorage::open(
        "127.0.0.1",
        2003,
        "root",
        "rewrite_it_in_rust",
        1,
        teloxide::dispatching::dialogue::serializer::Json,
    )
    .await
    .unwrap();
    test_skytable(storage).await;
}

#[tokio::test]
#[cfg_attr(not(CI_SKYTABLE), ignore)]
async fn test_skytable_bincode() {
    let storage = SkytableStorage::open(
        "127.0.0.1",
        2003,
        "root",
        "rewrite_it_in_rust",
        1,
        teloxide::dispatching::dialogue::serializer::Bincode,
    )
    .await
    .unwrap();
    test_skytable(storage).await;
}

#[tokio::test]
#[cfg_attr(not(CI_SKYTABLE), ignore)]
async fn test_skytable_cbor() {
    let storage = SkytableStorage::open(
        "127.0.0.1",
        2003,
        "root",
        "rewrite_it_in_rust",
        1,
        teloxide::dispatching::dialogue::serializer::Cbor,
    )
    .await
    .unwrap();
    test_skytable(storage).await;
}

type Dialogue = String;

macro_rules! test_dialogues {
    ($storage:expr, $_0:expr, $_1:expr, $_2:expr) => {
        assert_eq!(Arc::clone(&$storage).get_dialogue(ChatId(1)).await.unwrap(), $_0);
        assert_eq!(Arc::clone(&$storage).get_dialogue(ChatId(11)).await.unwrap(), $_1);
        assert_eq!(Arc::clone(&$storage).get_dialogue(ChatId(256)).await.unwrap(), $_2);
    };
}

async fn test_skytable<S>(storage: Arc<SkytableStorage<S>>)
where
    S: Send + Sync + Serializer<Dialogue> + 'static,
    <S as Serializer<Dialogue>>::Error: Debug + Display,
{
    test_dialogues!(storage, None, None, None);

    Arc::clone(&storage).update_dialogue(ChatId(1), "ABC".to_owned()).await.unwrap();
    Arc::clone(&storage).update_dialogue(ChatId(11), "DEF".to_owned()).await.unwrap();
    Arc::clone(&storage).update_dialogue(ChatId(256), "GHI".to_owned()).await.unwrap();

    test_dialogues!(
        storage,
        Some("ABC".to_owned()),
        Some("DEF".to_owned()),
        Some("GHI".to_owned())
    );

    Arc::clone(&storage).remove_dialogue(ChatId(1)).await.unwrap();
    Arc::clone(&storage).remove_dialogue(ChatId(11)).await.unwrap();
    Arc::clone(&storage).remove_dialogue(ChatId(256)).await.unwrap();

    test_dialogues!(storage, None, None, None);

    // Check that a try to remove a non-existing dialogue results in an error.
    assert!(matches!(
        Arc::clone(&storage).remove_dialogue(ChatId(1)).await.unwrap_err(),
        SkytableStorageError::DialogueNotFound
    ));
}
