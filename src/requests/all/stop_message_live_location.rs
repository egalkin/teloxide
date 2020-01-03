use serde::Serialize;

use crate::{
    network,
    requests::{Request, ResponseResult},
    types::{ChatOrInlineMessage, InlineKeyboardMarkup, Message},
    Bot,
};

/// Use this method to stop updating a live location message before live_period
/// expires. On success, if the message was sent by the bot, the sent Message is
/// returned, otherwise True is returned.
#[serde_with_macros::skip_serializing_none]
#[derive(Debug, Clone, Serialize)]
pub struct StopMessageLiveLocation<'a> {
    #[serde(skip_serializing)]
    bot: &'a Bot,

    #[serde(flatten)]
    chat_or_inline_message: ChatOrInlineMessage,

    /// A JSON-serialized object for a new inline keyboard.
    reply_markup: Option<InlineKeyboardMarkup>,
}

#[async_trait::async_trait]
impl Request for StopMessageLiveLocation<'_> {
    type Output = Message;

    async fn send(&self) -> ResponseResult<Message> {
        network::request_json(
            self.bot.client(),
            self.bot.token(),
            "stopMessageLiveLocation",
            &serde_json::to_string(self).unwrap(),
        )
        .await
    }
}

impl<'a> StopMessageLiveLocation<'a> {
    pub(crate) fn new(
        bot: &'a Bot,
        chat_or_inline_message: ChatOrInlineMessage,
    ) -> Self {
        Self {
            bot,
            chat_or_inline_message,
            reply_markup: None,
        }
    }

    pub fn chat_or_inline_message(mut self, val: ChatOrInlineMessage) -> Self {
        self.chat_or_inline_message = val;
        self
    }

    pub fn reply_markup(mut self, val: InlineKeyboardMarkup) -> Self {
        self.reply_markup = Some(val);
        self
    }
}
