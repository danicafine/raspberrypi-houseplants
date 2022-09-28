import pprint
from enum import IntEnum

from telegram import ForceReply, Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (   
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters
)

from helpers import clients,logging
from classes.mapping import Mapping
from classes.houseplant import Houseplant

logger = logging.set_logging('telegram_houseplant_bot')
config = clients.config()

PLANT_STATE = IntEnum('PlantState', [
    'ID',
    'GIVEN',
    'SCIENTIFIC',
    'COMMON',
    'TEMP_LOW',
    'TEMP_HIGH',
    'MOISTURE_LOW',
    'MOISTURE_HIGH',
    'COMMIT_PLANT'
])

MAPPING_STATE = IntEnum('MappingState', [
    'SENSOR_ID',
    'PLANT_ID',
    'COMMIT_MAPPING'
])


####################################################################################
#                                                                                  #
#                              UPDATE PLANT HANDLERS                               #
#                                                                                  #
####################################################################################

async def update_plant_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant'] = {}
        await update.message.reply_text(
            "Enter the plant id of the plant you'd like to update."
        )

        return PLANT_STATE.ID
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END

async def id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant']['plant_id'] = int(update.message.text)

        await update.message.reply_text(
                 "Please enter plant's given name."
             )

        return PLANT_STATE.GIVEN
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def given_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant']['given_name'] = update.message.text

        await update.message.reply_text(
                 "Please enter plant's scientific name."
             )

        return PLANT_STATE.SCIENTIFIC
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def scientific_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant']['scientific_name'] = update.message.text

        await update.message.reply_text(
                 "Please enter plant's common name."
             )

        return PLANT_STATE.COMMON
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def common_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['plant']['common_name'] = update.message.text

        await update.message.reply_text(
                 "Please enter plant's low temperature threshold in C or /skip to use the default."
             )

        return PLANT_STATE.TEMP_LOW
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def temp_low_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        # capture and store low temp data
        temp_low = update.message.text
        if temp_low != '/skip':
            # update state
            context.user_data['plant']['temperature_low'] = int(temp_low)
        else:
            context.user_data['plant']['temperature_low'] = 40

        await update.message.reply_text(
                 "Please enter plant's high temperature threshold in C or /skip to use the default."
             )

        return PLANT_STATE.TEMP_HIGH
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def temp_high_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        # capture and store high temp data
        temp_high = update.message.text
        if temp_high != '/skip':
            # update state
            context.user_data['plant']['temperature_high'] = int(temp_high)
        else:
            context.user_data['plant']['temperature_high'] = 100

        await update.message.reply_text(
                 "Please enter plant's low moisture threshold or /skip to use the default."
             )

        return PLANT_STATE.MOISTURE_LOW
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def moisture_low_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        # capture and store high temp data
        moisture_low = update.message.text
        if moisture_low != '/skip':
            # update state
            context.user_data['plant']['moisture_low'] = int(moisture_low)
        else:
            context.user_data['plant']['moisture_low'] = 25

        await update.message.reply_text(
                 "Please enter plant's high moisture threshold or /skip to use the default."
             )

        return PLANT_STATE.MOISTURE_HIGH
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def moisture_high_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        # capture and store high temp data
        moisture_high = update.message.text
        if moisture_high != '/skip':
            # update state
            context.user_data['plant']['moisture_high'] = int(moisture_high)
        else:
            context.user_data['plant']['moisture_high'] = 65

        # build up summary for confirmation
        summary = pprint.pformat(context.user_data['plant'])
        await update.message.reply_text(
            "Please confirm your metadata entry.\n\n"
            f"{summary}"
            "\nIs this correct? Reply /y to commmit the metadata entry or use /cancel to start over.", 
        )

        return PLANT_STATE.COMMIT_PLANT
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def commit_plant_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        try:
            send_metadata(context.user_data['plant'])
            context.user_data['plant'].clear()

            await update.message.reply_text(
                "You've confirmed your metadata entry, and it has been updated. "
            )
        except Exception as e:
            await update.message.reply_text(
                "Your metadata was not sent; please use /update_plant to re-try."
            )

        return ConversationHandler.END
    else:
        await update.message.reply_text(
                "You are not authorized to use this application."
            )
        return ConversationHandler.END


def send_metadata(metadata):
    # send metadata message
    try:
        # set up Kafka producer for houseplant metadata
        producer = clients.producer(clients.houseplant_serializer())
        value = Houseplant.dict_to_houseplant(metadata)

        k = str(metadata.get('plant_id'))
        logger.info("Publishing metadata message for key %s", k)
        producer.produce(config['topics']['houseplants'], key=k, value=value) 
    except Exception as e:
        logger.error("Got exception %s", e)
        raise e
    finally:
        producer.poll()
        producer.flush()


####################################################################################
#                                                                                  #
#                             UPDATE MAPPING HANDLERS                              #
#                                                                                  #
####################################################################################

async def update_mapping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['mapping'] = {}

        reply_keyboard = [
            ["0x36"], 
            ["0x37"], 
            ["0x38"], 
            ["0x39"]
        ]

        await update.message.reply_text(
            "Select the sensor ID for the mapping you'd like to update.",
            reply_markup=ReplyKeyboardMarkup(
                reply_keyboard, one_time_keyboard=True, input_field_placeholder="Sensor ID"
            )
        )

        return MAPPING_STATE.SENSOR_ID
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def sensor_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['mapping']['sensor_id'] = update.message.text

        await update.message.reply_text(
            f"Please enter the plant id you'd like to map to {update.message.text}.", 
            reply_markup=ReplyKeyboardRemove()
        )

        return MAPPING_STATE.PLANT_ID
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def plant_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        context.user_data['mapping']['plant_id'] = int(update.message.text)

        # build up summary for confirmation
        summary = pprint.pformat(context.user_data['mapping'])
        await update.message.reply_text(
            "Please confirm your mapping entry.\n\n"
            f"{summary}"
            "\nIs this correct? Reply /y to commmit the mapping entry or use /cancel to start over.", 
        )

        return MAPPING_STATE.COMMIT_MAPPING
    else:
        await update.message.reply_text(
            "You are not authorized to use this application."
        )
        return ConversationHandler.END


async def commit_mapping_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if config['telegram']['chat-id'] == update.message.chat_id:
        try:
            send_mapping(context.user_data['mapping'])
            context.user_data['mapping'].clear()

            await update.message.reply_text(
                "You've confirmed your mapping entry, and it has been updated. "
            )
        except Exception as e:
            await update.message.reply_text(
                "Your mapping entry was not sent; please use /update_mapping to re-try."
            )

        return ConversationHandler.END
    else:
        await update.message.reply_text(
                "You are not authorized to use this application."
            )
        return ConversationHandler.END


def send_mapping(mapping): 
    # send mapping message
    try:
        # set up Kafka producer for mappings
        producer = clients.producer(clients.mapping_serializer())
        value = Mapping.dict_to_mapping(mapping)

        k = str(mapping.get('sensor_id'))
        logger.info("Publishing mapping message for key %s", k)
        producer.produce(config['topics']['mappings'], key=k, value=value) 
    except Exception as e:
        logger.error("Got exception %s", e)
        raise e
    finally:
        producer.poll()
        producer.flush()


####################################################################################
#                                                                                  #
#                                 OTHER HANDLERS                                   #
#                                                                                  #
####################################################################################

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels and ends the conversation."""
    context.user_data.clear()

    await update.message.reply_text(
        "Cancelled metadata update."
    )

    return ConversationHandler.END


async def post_init(application: Application) -> None:
    await application.bot.set_my_commands([
        ('update_plant', "Update plant metadata"),
        ('update_mapping', "Update sensor-plant mapping")
        ])


####################################################################################

def main() -> None:
    # create the application and pass in bot token
    application = Application.builder().token(config['telegram']['api-token']).post_init(post_init).build()

    # define conversation handlers
    update_plant_handler = ConversationHandler(
        entry_points=[CommandHandler('update_plant', update_plant_command)],
        states={
            PLANT_STATE.ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, id_command),
                CommandHandler('cancel', cancel_command)
            ],
            PLANT_STATE.GIVEN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, given_command),
                CommandHandler('cancel', cancel_command)
            ],
            PLANT_STATE.SCIENTIFIC: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, scientific_command),
                CommandHandler('cancel', cancel_command)
            ],
            PLANT_STATE.COMMON: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, common_command),
                CommandHandler('cancel', cancel_command)
            ],
            PLANT_STATE.TEMP_LOW: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), temp_low_command), 
                CommandHandler('cancel', cancel_command)
            ],
            PLANT_STATE.TEMP_HIGH: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), temp_high_command), 
                CommandHandler('cancel', cancel_command)
            ],
            PLANT_STATE.MOISTURE_LOW: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), moisture_low_command), 
                CommandHandler('cancel', cancel_command)
            ],
            PLANT_STATE.MOISTURE_HIGH: [
                MessageHandler(filters.TEXT & (~filters.COMMAND | filters.Regex("^\/skip$")), moisture_high_command), 
                CommandHandler('cancel', cancel_command)
            ],
            PLANT_STATE.COMMIT_PLANT: [
                CommandHandler('y', commit_plant_command), 
                CommandHandler('n', cancel_command)
            ]
        },
        fallbacks=[CommandHandler('cancel', cancel_command)]
    )

    update_mapping_handler = ConversationHandler(
        entry_points=[CommandHandler('update_mapping', update_mapping_command)],
        states={
            MAPPING_STATE.SENSOR_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, sensor_id_command),
                CommandHandler('cancel', cancel_command)
            ],
            MAPPING_STATE.PLANT_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, plant_id_command),
                CommandHandler('cancel', cancel_command)
            ],
            MAPPING_STATE.COMMIT_MAPPING: [
                CommandHandler('y', commit_mapping_command), 
                CommandHandler('n', cancel_command)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_command)]
    )

    # add handlers
    application.add_handler(update_plant_handler)
    application.add_handler(update_mapping_handler)

    # run the bot application
    application.run_polling()


if __name__ == '__main__':
    main()