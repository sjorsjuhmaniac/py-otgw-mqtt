import re
from threading import Thread
from time import sleep, time
import logging
import collections
import copy, json

log = logging.getLogger(__name__)

# Default namespace for the topics. Will be overwritten with the value in
# config
pub_topic_namespace="otgw/value"
sub_topic_namespace="otgw/set"
ha_publish_namespace="homeassistant"

# Parse hex string to int
def hex_int(hex):
    return int(hex, 16)

# Pre-compile a regex to parse valid OTGW-messages
line_parser = re.compile(
    r'^(?P<source>[BART])(?P<type>[0-9A-F])(?P<res>[0-9A-F])'
    r'(?P<id>[0-9A-F]{2})(?P<data>[0-9A-F]{4})$'
)

# check valid commands
command_parser = re.compile(
    r'(?P<cmd>[\S]{2}):\s(?P<val>.*)'
)

r_three_char          = re.compile(r"^[a-zA-Z]{3}$")
r_single_char         = re.compile(r"^[a-zA-Z]$")
r_single_digit        = re.compile(r"^[0-1]$")
r_single_digit_char   = re.compile(r"^[0-1]$|^[a-zA-Z]$")
r_float               = re.compile(r"^\d{1,2}\.\d{2}$")
r_int                 = re.compile(r"^\d+$")

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def is_int(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

command_generators={
    "/room_setpoint_temporary/setpoint": \
        lambda _ :"TT={:.2f}".format(float(_) if is_float(_) else 0),
    "/room_setpoint_constant/setpoint":  \
        lambda _ :"TC={:.2f}".format(float(_) if is_float(_) else 0),
    "/outside_temperature/setpoint":     \
        lambda _ :"OT={:.2f}".format(float(_) if is_float(_) else 99),
    "/dhw_enable/state":        \
        lambda _ :"HW={}".format('1' if _ in true_values else '0' if _ in false_values else 'T'),
    "/dhw_setpoint/setpoint":   \
        lambda _ :"SW={:.2f}".format(float(_) if is_float(_) else 60),
    "/ch_enable/state":  \
        lambda _ :"CH={}".format('0' if _ in false_values else '1'),
    "/max_ch_water_setpoint/setpoint":   \
        lambda _ :"SH={:.2f}".format(float(_) if is_float(_) else 60),
    "/control_setpoint/setpoint":   \
        lambda _ :"CS={:.2f}".format(float(_) if is_float(_) else 60),
    "/max_relative_modulation_level/level":  \
        lambda _ :"MM={:d}".format(int(_) if is_int(_) else 100),
    "/cmd":  \
        lambda _ :_.strip(),
    # TODO: "set/otgw/raw/+": lambda _ :publish_to_otgw("PS", _)
}

def float_cmd_generator(cmd, val):
    r"""
    
    """
    yield ("/command_response{}".format(cmd), float(val), )

def int_cmd_generator(cmd, val):

    yield ("/command_response{}".format(cmd), int(val), )

def char_cmd_generator(cmd, val):

    yield ("/command_response{}".format(cmd), val, )


def flags_msg_generator(ot_id, val):
    r"""
    Generate the pub-messages from a boolean value.

    Currently, only flame status is supported. Any other items will be returned
    as-is.

    Returns a generator for the messages
    """
    yield ("{}".format(ot_id), int(val), )
    if(ot_id == "/master_slave_status"):

        ####
        # data is 2 byte
        # 0000 0000
        # |       |
        # master   slave
        ####

        for bit, bit_name in master_slave_status_bits.items():

            yield ("{}".format(bit_name),
                   int(val & ( 1 << bit ) > 0), )

def float_msg_generator(ot_id, val):
    r"""
    Generate the pub-messages from a float-based value

    Returns a generator for the messages
    """
    yield ("{}".format(ot_id), round(val/float(256), 2), )

def int_msg_generator(ot_id, val):
    r"""
    Generate the pub-messages from an integer-based value

    Returns a generator for the messages
    """
    yield ("{}".format(ot_id), val, )

def other_msg_generator(source, ttype, res, did, data):
    r"""
    Generate the pub-messages from an unknown message.
    Casts value as string.

    Returns a generator for the messages
    """
    yield ("/{}/{}/{}/{}/{}".format('unknown', source, ttype, res, did), str(data), )

def get_messages(message):
    r"""
    Generate the pub-messages from the supplied OT-message

    Returns a generator for the messages
    """

    #
    info = line_parser.match(message)
    command = command_parser.match(message)
    #

    # garbage
    if info is None and command is None:
        if message:
            log.error("Did not understand message: '{}'".format(message))
        return iter([])

    if command is not None:
        log.error("command is not None: {}".format(command))
        (cmd, val) = command.groups()
        log.error("command is not None: {} {}".format(cmd, val))
        if cmd in command_response_errors:
        	# need to create an error topic and publish to that
        	log.error("Received an error response: '{}'".format(message))
        	return iter([])

        if cmd not in command_response:
        	log.error("Received an unknown command response: '{}'".format(message))
        	return iter([])

        # finally... some good stuff
        log.error("Received valid command response: '{}'".format(message))

        # need to somehow figure out of this is partial msg of full.....
        # hope these specific regex' are good enough
        cmd_name, parser, regex = command_response[cmd]
        log.error("good command info: {} {} {}".format(cmd_name, parser, regex))

        if regex.match(val):
            log.error("matched regex: {}".format(val))
            return parser(cmd_name, val)
        else:
            log.error("Did not match command regex")
            return iter([])



    if info is not None:
        (source, ttype, res, did, data) = \
        map(lambda f, d: f(d),
            (str, lambda _: hex_int(_) & 7, hex_int, hex_int, hex_int),
            info.groups())

        if source not in ('B', 'T', 'A') \
            or ttype not in (1,4):
            return iter([])
        if did not in opentherm_ids:
            return other_msg_generator(source, ttype, res, did, data)

        id_name, parser = opentherm_ids[did]
        return parser(id_name, data)


# Map the opentherm ids (named group 'id' in the line parser regex) to
# discriptive names and message creators. I put this here because the
# referenced generators have to be assigned first
# We're using these names as topic's to push the mqtt message to
opentherm_ids = {
    # flame status is special case... multiple bits of data. see flags_msg_generator
	0:   ("/master_slave_status",flags_msg_generator,),
	1:   ("/control_setpoint/setpoint",float_msg_generator,),
	9:   ("/remote_override_setpoint/setpoint",float_msg_generator,),
	14:  ("/max_relative_modulation_level/level",float_msg_generator,),
	16:  ("/room_setpoint/setpoint",float_msg_generator,),
	17:  ("/relative_modulation_level/level",float_msg_generator,),
	18:  ("/ch_water_pressure/pressure",float_msg_generator,),
	24:  ("/room_temperature/temperature",float_msg_generator,),
	25:  ("/boiler_water_temperature/temperature",float_msg_generator,),
	26:  ("/dhw_temperature/temperature",float_msg_generator,),
	27:  ("/outside_temperature/temperature",float_msg_generator,),
	28:  ("/return_water_temperature/temperature",float_msg_generator,),
	56:  ("/dhw_setpoint/setpoint",float_msg_generator,),
	57:  ("/max_ch_water_setpoint/setpoint",float_msg_generator,),
	116: ("/burner_starts/count",int_msg_generator,),
	117: ("/ch_pump_starts/count",int_msg_generator,),
	118: ("/dhw_pump_starts/count",int_msg_generator,),
	119: ("/dhw_burner_starts/count",int_msg_generator,),
	120: ("/burner_operation_hours/hours",int_msg_generator,),
	121: ("/ch_pump_operation_hours/hours",int_msg_generator,),
	122: ("/dhw_pump_valve_operation_hours/hours",int_msg_generator,),
	123: ("/dhw_burner_operation_hours/hours",int_msg_generator,),
}

# { <bit>, <name>}
# We're using these names as topic's to push the mqtt message to
master_slave_status_bits = {
    0:  "/fault/state",
    1:  "/ch_active/state",
    2:  "/dhw_active/state",
    3:  "/flame_on/state",
    4:  "/cooling_active/state",
    5:  "/ch2_active/state",
    6:  "/diagnostic_indication/state",
    7:  "/bit_7/state",
    8:  "/ch_enabled/state",
    9:  "/dhw_enabled/state",
    10: "/cooling_enabled/state",
    11: "/otc_active/state",
    12: "/ch2_enabled/state",
    13: "/bit_13/state",
    14: "/bit_14/state",
    15: "/bit_15/state",
    }


# list of otgw error responses 
command_response_errors = [
    "NG", # - No Good    The command code is unknown. 
    "SE", # - Syntax Error    The command contained an unexpected character or was incomplete. 
    "BV", # - Bad Value    The command contained a data value that is not allowed. 
    "OR", # - Out of Range    A number was specified outside of the allowed range. 
    "NS", # - No Space    The alternative Data-ID could not be added because the table is full. 
    "NF", # - Not Found    The specified alternative Data-ID could not be removed because it does not exist in the table. 
    "OE", # - Overrun Error.    The processor 
]

# Map the otgw commands (named group 'cmd' in the line parser regex) to
# discriptive names and message creators. I put this here because the
# referenced generators have to be assigned first
# We're using these names as topic's to push the mqtt message to
# 
# OTGW firmware v 4.2.5

command_response = {
    "TT": ("/room_setpoint_temporary/setpoint",float_cmd_generator, r_float), # 1.00 12.50 19.80 
    # TT=temperature
    #     Temperature, Temporary - Temporarily change the thermostat setpoint. The thermostat program will resume at the next programmed setpoint change. Values between 0.0 and 30.0 are allowed. A value of 0 indicates no remote override is to be applied.
    #     Examples: TT=19.5, TT=0 

    "TC": ("/room_setpoint_constant/setpoint",float_cmd_generator, r_float),
    # TC=temperature
    #     Temperature, Constant - Change the thermostat setpoint. The thermostat program will not change this setting. Values between 0.0 and 30.0 are allowed. A value of 0 cancels the remote override.
    #     Examples: TC=16.0, TC=0 

    "OT": ("/outside_temperature/setpoint",float_cmd_generator, r_float),
    # OT=temperature
    #     Outside temperature - Configures the outside temperature to send to the thermostat. Allowed values are between -40.0 and +64.0, although thermostats may not display the full range. Specify a value above 64 (suggestion: 99) to clear a previously configured value.
    #     Examples: OT=-3.5, OT=99 

#TODO, should be set to /clock but set the /char for now
    "SC": ("/set_clock/char",char_cmd_generator, r"^\d{1,2}:\d{2}\/\d$"),
    # SC=time/day
    #     Set Clock - Change the time and day of the week of the thermostat. The gateway will send the specified time and day of the week in response to the next time and date message from the thermostat. The time must be specified as HH:MM. The day of the week must be specified as a single digit between 1 (Monday) and 7 (Sunday).
    #     Examples: SC=9:00/1, SC=23:59/4 

    "HW": ("/dhw_enable/state",char_cmd_generator, r_single_digit_char),
    # HW=state
    #     Hot Water - Control the domestic hot water enable option. If the boiler has been configured to let the room unit control when to keep a small amount of water preheated, this command can influence that. A state of 0 or 1 will tell the boiler whether or not to keep the water warm. Any other single character causes the gateway to let the thermostat control the boiler. Possible values are 0, 1, or any other single character.
    #     Examples: HW=1, HW=T 

    "PR": ("/print_report/char",char_cmd_generator, r_single_char),
    # PR=item
    #     Print Report - Request the gateway to report some information item. The following items are currently defined:

    #     A    About opentherm gateway (prints the welcome message)
    #     B    Build date and time
    #     C    The clock speed the code was compiled for (4 MHz)
    #     G    Configured functions for the two GPIO pins. The response will be 2 digits that represent the functions of GPIO A and GPIO B respectively.
    #     I    Current state of the two GPIO pins. The response will be 2 digits that represent the level (0 or 1) of GPIO A and GPIO B respectively.
    #     L    Configured functions for all 6 LEDS. The response consists of 6 letters representing the functions of LED A through LED F.
    #     M    Gateway mode. G=Gateway, M=Monitor.
    #     O    Report the setpoint override value
    #     P    Current Smart-Power mode (low/medium/high).
    #     R    The state of the automatic Remeha thermostat detection.
    #     S    The configured setback temperature.
    #     T    Tweaks. Reports the state of the ignore transitions and override in high byte settings.
    #     V    Report the reference voltage setting
    #     W    Report the domestic hot water setting 
    #     Examples: PR=L, PR=A 

    "PS": ("/print_summary/state",int_cmd_generator, r_int),
    # PS=state
    #     Print Summary - The opentherm gateway normally prints every opentherm message it receives, as well as the modified messages it transmits. In some applications it may be more useful to only get a report of the latest values received for the most interesting parameters on demand. Issuing a "PS=1" command will stop the reports for each message and print one line with the following values:

    #         Status (MsgID=0) - Printed as two 8-bit bitfields
    #         Control setpoint (MsgID=1) - Printed as a floating point value
    #         Remote parameter flags (MsgID= 6) - Printed as two 8-bit bitfields
    #         Maximum relative modulation level (MsgID=14) - Printed as a floating point value
    #         Boiler capacity and modulation limits (MsgID=15) - Printed as two bytes
    #         Room Setpoint (MsgID=16) - Printed as a floating point value
    #         Relative modulation level (MsgID=17) - Printed as a floating point value
    #         CH water pressure (MsgID=18) - Printed as a floating point value
    #         Room temperature (MsgID=24) - Printed as a floating point value
    #         Boiler water temperature (MsgID=25) - Printed as a floating point value
    #         DHW temperature (MsgID=26) - Printed as a floating point value
    #         Outside temperature (MsgID=27) - Printed as a floating point value
    #         Return water temperature (MsgID=28) - Printed as a floating point value
    #         DHW setpoint boundaries (MsgID=48) - Printed as two bytes
    #         Max CH setpoint boundaries (MsgID=49) - Printed as two bytes
    #         DHW setpoint (MsgID=56) - Printed as a floating point value
    #         Max CH water setpoint (MsgID=57) - Printed as a floating point value
    #         Burner starts (MsgID=116) - Printed as a decimal value
    #         CH pump starts (MsgID=117) - Printed as a decimal value
    #         DHW pump/valve starts (MsgID=118) - Printed as a decimal value
    #         DHW burner starts (MsgID=119) - Printed as a decimal value
    #         Burner operation hours (MsgID=120) - Printed as a decimal value
    #         CH pump operation hours (MsgID=121) - Printed as a decimal value
    #         DHW pump/valve operation hours (MsgID=122) - Printed as a decimal value
    #         DHW burner operation hours (MsgID=123) - Printed as a decimal value 

    #     A new report can be requested by repeating the "PS=1" command.
    #     Examples: PS=1, PS=0 

    "GW": ("/gateway_state/char",char_cmd_generator, r_single_digit_char),
    # GW=state
    #     GateWay - The opentherm gateway starts up in back-to-back mode. While this is the most useful mode of operation, it also means that the firmware must be able to decode the requests received from the thermostat before it can send them on to the boiler. The same is true for responses from the boiler back to the thermostat. By changing this setting to "0" (monitor mode), the received signal level is passed through to the output driver without any processing. This can be a useful diagnostic tool when there are communication problems immediately after the gateway has been built. See the troubleshooting section for more information. This command can also be used to reset the gateway by specifying "R" as the state value.
    #     Examples: GW=1, GW=R 

    "LA": ("/led_a/char",char_cmd_generator, r_single_char),
    "LB": ("/led_b/char",char_cmd_generator, r_single_char),
    "LC": ("/led_c/char",char_cmd_generator, r_single_char),
    "LD": ("/led_d/char",char_cmd_generator, r_single_char),
    "LE": ("/led_e/char",char_cmd_generator, r_single_char),
    "LF": ("/led_f/char",char_cmd_generator, r_single_char),
    # LA=function
    # LB=function
    # LC=function
    # LD=function
    # LE=function
    # LF=function
    #     LED A / LED B / LED C / LED D / LED E / LED F - These commands can be used to configure the functions of the six LEDs that can optionally be connected to pins RB3/RB4/RB6/RB7 and the GPIO pins of the PIC. The following functions are currently available:

    #     R    Receiving an Opentherm message from the thermostat or boiler
    #     X    Transmitting an Opentherm message to the thermostat or boiler
    #     T    Transmitting or receiving a message on the master interface
    #     B    Transmitting or receiving a message on the slave interface
    #     O    Remote setpoint override is active
    #     F    Flame is on
    #     H    Central heating is on
    #     W    Hot water is on
    #     C    Comfort mode (Domestic Hot Water Enable) is on
    #     E    Transmission error has been detected
    #     M    Boiler requires maintenance
    #     P    Raised power mode active on thermostat interface. 
    #     Examples: LC=F, LD=M 


    "GA": ("/gpio_a/id",int_cmd_generator, r_int),
    "GB": ("/gpio_b/id",int_cmd_generator, r_int),
    # GA=function
    # GB=function
    #     GPIO A / GPIO B - These commands configure the functions of the two GPIO pins of the gateway. The following functions are available:

    #      0   No function, default for both ports on a freshly flashed chip.
    #      1   Ground - A permanently low output (0V). Could be used for a power LED.
    #      2   Vcc - A permanently high output (5V). Can be used as a short-proof power supply for some external circuitry used by the other GPIO port.
    #      3   LED E - An additional LED if you want to present more than 4 LED functions.
    #      4   LED F - An additional LED if you want to present more than 5 LED functions.
    #      5   Home - Set thermostat to setback temperature when pulled low.
    #      6   Away - Set thermostat to setback temperature when pulled high.
    #      7   DS1820 (GPIO port B only) - Data line for a DS18S20 or DS18B20 temperature sensor used to measure the outside temperature. A 4k7 resistor should be connected between GPIO port B and Vcc. 
    #     Examples: GA=2, GB=7 

    "SB": ("/setback_temperature/setpoint",float_cmd_generator, r_float),
    # SB=Data-ID
    #     SetBack temperature - Configure the setback temperature to use in combination with GPIO functions HOME (5) and AWAY (6). Note: The SB command may need to store 2 bytes in EEPROM. This takes more time than it takes to transfer a command over the serial interface. If you immediately follow the SB command by more commands that store configuration data in EEPROM, the gateway may not be able to handle all commands. To avoid any problems when sending a sequence of configuration commands, send the SB command last.
    #     Examples: SB=15, SB=16.5 

    "AA": ("/data_id_alternative_delete/id",int_cmd_generator, r_int),
    # AA=Data-ID
    #     Add Alternative - Add the specified Data-ID to the list of alternative commands to send to the boiler instead of a Data-ID that is known to be unsupported by the boiler. Alternative Data-IDs will always be sent to the boiler in a Read-Data request message with the data-value set to zero. The table of alternative Data-IDs is stored in non-volatile memory so it will persist even if the gateway has been powered off. Data-ID values from 1 to 255 are allowed.
    #     Examples: AA=33, AA=117 

    "DA": ("/data_id_alternative_add/id",int_cmd_generator, r_int),
    # DA=Data-ID
    #     Delete Alternative - Remove the specified Data-ID from the list of alternative commands. Only one occurrence is deleted. If the Data-ID appears multiple times in the list of alternative commands, this command must be repeated to delete all occurrences. The table of alternative Data-IDs is stored in non-volatile memory so it will persist even if the gateway has been powered off. Data-ID values from 1 to 255 are allowed.
    #     Examples: DA=116, DA=123 

    "UI": ("/data_id_unknown/id",int_cmd_generator, r_int),
    # UI=Data-ID
    #     Unknown ID - Inform the gateway that the boiler doesn't support the specified Data-ID, even if the boiler doesn't indicate that by returning an Unknown-DataId response. Using this command allows the gateway to send an alternative Data-ID to the boiler instead.
    #     Examples: UI=18, UI=6 

    "KI": ("/data_id_known/id",int_cmd_generator, r_int),
    # KI=Data-ID
    #     Known ID - Start forwarding the specified Data-ID to the boiler again. This command resets the counter used to determine if the specified Data-ID is supported by the boiler.
    #     Examples: KI=18, KI=123 

    "PM": ("/data_id_priority/id",int_cmd_generator, r_int),
    # PM=Data-ID
    #     Priority Message - Specify a one-time priority message to be sent to the boiler at the first opportunity. If the specified message returns the number of Transparent Slave Parameters (TSPs) or Fault History Buffers (FHBs), the gateway will proceed to request those TSPs or FHBs.
    #     Example: PM=10, PM=72 

    "SR": ("/data_id_response_set/char",char_cmd_generator),
    # SR=Data-ID:data
    #     Set Response - Configure a response to send back to the thermostat instead of the response produced by the boiler. The data argument is either one or two bytes separated by a comma.
    #     Example: SR=18:1,205, SR=70:14 

    "CR": ("/data_id_response_clear/id",int_cmd_generator, r_int),
    # CR=Data-ID
    #     Clear Response - Clear a previously configured response to send back to the thermostat.
    #     Example: CR=18, CR=70 

    "SH": ("/max_ch_water_setpoint/setpoint",char_cmd_generator),
    # SH=temperature
    #     Setpoint Heating - Set the maximum central heating setpoint. This command is only available with boilers that support this function.
    #     Examples: SH=72.5, SH=+20 

    "SW": ("/dhw_setpoint/setpoint",char_cmd_generator),
    # SW=temperature
    #     Setpoint Water - Set the domestic hot water setpoint. This command is only available with boilers that support this function.
    #     Examples: SW=60, SW=+40.0 

    "MM": ("/max_relative_modulation_level/level",char_cmd_generator),
    # MM=percentage
    #     Maximum Modulation - Override the maximum relative modulation from the thermostat. Valid values are 0 through 100. Clear the setting by specifying a non-numeric value.
    #     Examples: MM=100, MM=T 

    "CS": ("/control_setpoint/setpoint",float_cmd_generator, r_float),
    # CS=temperature
    #     Control Setpoint - Manipulate the control setpoint being sent to the boiler. Set to 0 to pass along the value specified by the thermostat. To stop the boiler heating the house, set the control setpoint to some low value and clear the CH enable bit using the CH command.
    #     Warning: manipulating these values may severely impact the control algorithm of the thermostat, which may cause it to start heating much too early or too aggressively when it is actually in control.
    #     Example: CS=45.8, CS=0 

    "CH": ("/ch_enable/state",int_cmd_generator, r_int),
    # CH=state
    #     Central Heating - When using external control of the control setpoint (via a CS command with a value other than 0), the gateway sends a CH enable bit in MsgID 0 that is controlled using the CH command. Initially this bit is set to 1. When external control of the control setpoint is disabled (CS=0), the CH enable bit is controlled by the thermostat.
    #     Example: CH=0, CH=1 

    "VS": ("/ventilation_setpoint/level",int_cmd_generator, r_int),
    # VS=percentage
    #     Ventilation Setpoint - Configure a ventilation setpoint override value.
    #     Example: VS=25, VS=100 

    "RS": ("/reset_counter/char",char_cmd_generator, r_three_char),
    # RS=counter
    #     Reset - Clear boiler counter, if supported by the boiler. Available counter names are:
    #     HBS     Central heating burner starts
    #     HBH     Central heating burner operation hours
    #     HPS     Central heating pump starts
    #     HPH     Central heating pump operation hours
    #     WBS     Domestic hot water burner starts
    #     WBH     Domestic hot water burner operation hours
    #     WPS     Domestic hot water pump starts
    #     WPH     Domestic hot water pump operation hours

    "IT": ("/ignore_transitions/state",int_cmd_generator, r_int),
    # IT=state
    #     Ignore Transitions - If the opentherm signal doesn't cleanly transition from one level to the other, the circuitry may detect multiple transitions when there should only be one. When this setting is off (IT=0), the gateway will report "Error 01" for those cases. With this setting on (IT=1), any rapid bouncing of the signal is ignored. This is the default.
    #     Examples: IT=0, IT=1 

    "OH": ("/override_high_byte/state",int_cmd_generator, r_int),
    # OH=state
    #     Override in High byte - The Opentherm specification contains contradicting information about which data byte of Data-ID 100 should hold the override bits. When this setting is off (OH=0), the gateway will only put the bits in the low byte. When the setting is on (OH=1), the bits are copied to the high byte so they appear in both bytes. This is the default.
    #     Examples: OH=0, OH=1 

    "FT": ("/force_thermostat/char",char_cmd_generator, r_single_char),
    # FT=model
    #     Force Thermostat - To be able to apply special treatment required by some thermostat models, the gateway will try to auto-detect which thermostat is connected. In some cases it is unable to determine this correctly. This configuration option can then be used to force the model. Valid models are: 'C' (Remeha Celcia 20) and 'I' (Remeha iSense). Any other letter restores the default auto-detect functionality.
    #     Examples: FT=C, FT=D 

    "VR": ("/voltage_reference/id",int_cmd_generator, r_int),
    # VR=level
    #     Voltage Reference - Change the reference voltage used as a threshold for the comparators. This configuration option is stored in non-volatile memory so it will persist even if the gateway has been powered off. The level must be specified as a single digit according to the following table:
    #     0   1   2   3   4   5   6   7   8   9
    #     0.625V  0.833V  1.042V  1.250V  1.458V  1.667V  1.875V  2.083V  2.292V  2.500V
    #     The normal value is 3.
    #     Examples: VR=3, VR=4 

    "DP": ("/debug_pointer/id",int_cmd_generator, r_int),
    # DP=address
    #     Debug Pointer - Set the debug pointer to a file register. If the debug pointer has been set to a value other than 00, the contents of the selected file register will be reported over the serial interface after each received OpenTherm message. The address must be specified as two hexadecimal digits. Setting the pointer to 00 switches off the debug reports.
    #     Examples: DP=1F, DP=00 
}

# Flip list
# {
# ...
#     "DP": ("/debug_pointer/id",int_cmd_generator, r_int),
# ...
#     "/debug_pointer/id": ("DP",int_cmd_generator, r_int),
# ...
# }
topic_response = {v[0]: (k,v[1:]) for k,v in command_response.items()}



def cleanNullTerms(d):
    clean = {}
    for k, v in d.items():
        if isinstance(v, dict):
            nested = cleanNullTerms(v)
            if len(nested.keys()) > 0:
                clean[k] = nested
        elif v is not None:
            clean[k] = v
    return clean


def build_ha_config_data (config):
    # base template
    # id
    # char
    payload_sensor = {
        "availability_topic": pub_topic_namespace,
        "device":
            {
            "connections": None,
            "identifiers": ["{}-{}:{}".format(config['mqtt']['client_id'], config['otgw']['host'], config['otgw']['port'])],
            "manufacturer": "Schelte Bron",
            "model": "otgw-nodo",
            "name": "OpenTherm Gateway ({})".format(config['mqtt']['client_id']),
            "sw_version": None,
            "via_device": None
            },
        "device_class": None,
        "expire_after": None,
        "force_update": 'True',
        "icon": None,
        "json_attributes_template": None,
        "json_attributes_topic": None,
        "name": None,
        "payload_available": None,
        "payload_not_available": None,
        "qos": None,
        "state_topic": None,
        "unique_id": None,
        "unit_of_measurement": None,
        "value_template": None,
    }
    # deepcopy
    payload_switch = copy.deepcopy(payload_sensor)
    payload_switch = {**payload_switch, 
        **{
        "availability":
            {
            "payload_available": None,
            "payload_not_available": None,
            "topic": None,
            },
        "availability_mode": None,
        "command_topic": None,
        "optimistic": None,
        "payload_off": None,
        "payload_on": None,
        "retain": None,
        "state_off": None,
        "state_on": None,
        "payload_off": 0,
        "payload_on": 1,
        }
    }
    del payload_switch["unit_of_measurement"]
    del payload_switch['device_class']
    del payload_switch["expire_after"]
    del payload_switch["force_update"]


    # deepcopy
    payload_climate = copy.deepcopy(payload_sensor)
    payload_climate = {**payload_climate, 
        **{
        # "action_template": None,
        # "action_topic": None,
        # "aux_command_topic": None,
        # "aux_state_template": None,
        # "aux_state_topic": None,
        # "away_mode_command_topic": None,
        # "away_mode_state_template": None,
        # "away_mode_state_topic": None,
        "current_temperature_topic": pub_topic_namespace+'/room_temperature/temperature',
        "current_temperature_template": None,
        "initial": '18',
        "max_temp": '24',
        "min_temp": '16',
        # "mode_command_topic": None,
        "mode_state_template": "{% if value == '1' %}heat{% else %}off{% endif %}",
        "mode_state_topic": pub_topic_namespace+'/ch_enabled/state',
        "modes": ['off', 'heat'],
        "precision": 0.1,
        "retain": None,
        "send_if_off": None,
        # using temporary allows local thermostat override. use /constant to block
        # room thermostat input
        "temperature_command_topic": sub_topic_namespace+'/room_setpoint_temporary/setpoint',
        "temperature_state_template": None,
        "temperature_state_topic": pub_topic_namespace+'/room_setpoint/setpoint',
        "temperature_unit": "C",
        "temp_step": "0.5", 
        "availability":
            {
            "payload_available": None,
            "payload_not_available": None,
            "topic": None,
            },
        "payload_off": 0,
        "payload_on": 1,
        }
    }
    del payload_climate["expire_after"]
    del payload_climate["force_update"]
    del payload_climate["icon"]
    del payload_climate["state_topic"]
    del payload_climate["unit_of_measurement"]

    # deepcopy
    payload_binary_sensor = copy.deepcopy(payload_sensor)
    payload_binary_sensor = {**payload_binary_sensor, 
        **{
        "availability":
            {
            "payload_available": None,
            "payload_not_available": None,
            "topic": None,
            },
        "off_delay": None,
        "payload_off": 0,
        "payload_on": 1,
        }
    }
    del payload_binary_sensor["unit_of_measurement"]
    payload_binary_sensor['device_class'] = 'heat'

    # deepcopy
    payload_sensor_temperature = copy.deepcopy(payload_sensor)
    payload_sensor_temperature['device_class'] = 'temperature'
    payload_sensor_temperature['unit_of_measurement'] = 'C'

    payload_sensor_hours = copy.deepcopy(payload_sensor)
    payload_sensor_hours['device_class'] = None
    payload_sensor_hours['icon'] = 'mdi:clock'
    payload_sensor_hours['unit_of_measurement'] = 'Hours'

    payload_sensor_pressure = copy.deepcopy(payload_sensor)
    payload_sensor_pressure['device_class'] = 'pressure'
    payload_sensor_pressure['unit_of_measurement'] = 'Bar'


    payload_sensor_count = copy.deepcopy(payload_sensor)
    payload_sensor_count['device_class'] = None
    payload_sensor_count['icon'] = 'mdi:counter'
    payload_sensor_count['unit_of_measurement'] = 'x'

    payload_sensor_level = copy.deepcopy(payload_sensor)
    payload_sensor_level['device_class'] = None
    payload_sensor_level['icon'] = 'mdi:percent'
    payload_sensor_level['unit_of_measurement'] = '%'

    payload_mapping = {
        "setpoint": {'ha_type': 'sensor', 'payload': payload_sensor_temperature},
        "temperature": {'ha_type': 'sensor', 'payload': payload_sensor_temperature},
        "hours": {'ha_type': 'sensor', 'payload': payload_sensor_hours},
        "count": {'ha_type': 'sensor', 'payload': payload_sensor_count},
        "level": {'ha_type': 'sensor', 'payload': payload_sensor_level},
        "state": {'ha_type': 'binary_sensor', 'payload': payload_binary_sensor},
        "pressure": {'ha_type': 'sensor', 'payload': payload_sensor_pressure},
        "id": {'ha_type': 'sensor', 'payload': payload_sensor},
        "char": {'ha_type': 'sensor', 'payload': payload_sensor},
    }


    #########
    #########
    #########

    data = []

    # data.append( {'topic': "{}/climate/thermostat/config".format(ha_publish_namespace), 'payload': ''})
    # add thermostat entity
    payload_climate['name'] = "{}_Thermostat".format(config['mqtt']['client_id'])
    payload_climate['unique_id'] = "{}_thermostat".format(config['mqtt']['client_id'])
    data.append( {'topic': "{}/climate/{}/thermostat/config".format(ha_publish_namespace, payload_climate['unique_id'] ), 'payload': json.dumps(cleanNullTerms(payload_climate)) })

    # add switches
    for full_name in command_generators:
        # just continue if we have a main group like the 'status bits' group
        if full_name.count('/') == 1:
            log.error("Inco")
            continue
        # command generators
        elif full_name.count('/') == 2:
            name, otgw_type = full_name[1:].split("/")
            log.error("Incoand_generators: {} {}".format(name, otgw_type))
        else:
            log.error("Incorrect entity item in command_generators: {}".format(full_name))
            continue

        #only 'state' type for now
        if otgw_type != "state":
            log.error("blah : {} {}".format(name, otgw_type))
            log.error("blah : {} {}".format(name, (otgw_type != "state")))
            continue

        log.error("blah command_generators: {}{}".format(name, otgw_type))

        payload = copy.deepcopy(payload_switch)
        ha_type = 'switch'
        # **{
        # "availability":
        #     {
        #     "payload_available": None,
        #     "payload_not_available": None,
        #     "topic": None,
        #     },
        # "availability_mode": None,
        # "command_topic": None,
        # "optimistic": None,
        # "payload_off": None,
        # "payload_on": None,
        # "retain": None,
        # "state_off": None,
        # "state_on": None,
        # "payload_off": 0,
        # "payload_on": 1,

        payload['command_topic'] = "{}{}".format(sub_topic_namespace,full_name)
        payload['name'] = "{}_{}".format(config['mqtt']['client_id'],name)
        # need to add the mqtt client name here to make it truely unique
        payload['unique_id'] = "{}_{}_{}".format(config['mqtt']['client_id'],ha_type,name)
        payload['state_topic'] = "{}{}".format(pub_topic_namespace, full_name)
        publish_topic = "{}/{}/{}/{}/config".format(ha_publish_namespace, ha_type, payload['unique_id'], name)
        payload = cleanNullTerms(payload)
        payload = json.dumps(payload)
        data.append( {'topic': publish_topic, 'payload': payload, 'retain':'True'})
        log.error(str({'topic': publish_topic, 'payload': payload, 'retain':'True'}))

    # build list of all entities, use their names
    entity_list = []
    entity_list += [opentherm_ids[x][0] for x in opentherm_ids]                         # opentherm_ids full names
    entity_list += [master_slave_status_bits[x] for x in master_slave_status_bits]      # master_slave_status_bits full names
    entity_list += ['/command_response'+command_response[x][0] for x in command_response]                   # command response full names, need to be prefixed not to override the opentherm sensor stuff
    # todo: setpoint entities for the water temp setpoints

    for full_name in entity_list:
        
        # dont include id 0: master_slave_status

        # otgw_type = full_name.split("/")[-1]
        # name = full_name.split("/")[0]
        log.error("entity item: {}".format(full_name))


        # just continue if we have a main group like the 'status bits' group
        if full_name.count('/') == 1:
            continue
        # sensors and stuff
        elif full_name.count('/') == 2:
            name, otgw_type = full_name[1:].split("/")
        # command_responses 
        elif full_name.count('/') == 3:
            base, name, otgw_type = full_name[1:].split("/")
        else:
            log.error("Incorrect entity item: {}".format(full_name))
            continue

        # don't include unsupported items        
        if otgw_type not in payload_mapping:
            continue

        ha_type = payload_mapping[otgw_type]['ha_type']
        payload = copy.deepcopy(payload_mapping[otgw_type]['payload'])

        payload['name'] = "{}_{}".format(config['mqtt']['client_id'],name)
        # need to add the mqtt client name here to make it truely unique
        payload['unique_id'] = "{}_{}".format(config['mqtt']['client_id'],name)
        payload['state_topic'] = "{}{}".format(pub_topic_namespace, full_name)
        publish_topic = "{}/{}/{}/{}/config".format(ha_publish_namespace, ha_type, payload['unique_id'], name)
        payload = cleanNullTerms(payload)
        payload = json.dumps(payload)
        data.append( {'topic': publish_topic, 'payload': payload, 'retain':'True'})

    return data


class OTGWClient(object):
    r"""
    An abstract OTGW client.

    This class can be used to create implementations of OTGW clients for
    different types of communication protocols and technologies. To create a
    full implementation, only four methods need to be implemented.
    """
    def __init__(self, listener, **kwargs):
        self._worker_running = False
        self._listener = listener
        self._worker_thread = None
        self._send_buffer = collections.deque()
        self._command_feedback_required = kwargs['command_feedback_required']
        self._command_max_retries = kwargs['command_max_retries']
        self._command_timeout = kwargs['command_timeout']

        self.IN_CONFIRMATION = False
        if self._command_feedback_required:
            self.stored_commands = {}
            # self.stored_commands ['/otgw9/set/room_setpoint_temporary/setpoint'] = {'received': int(time()), 'data': b'18', 'count': 0}

    def open(self):
        r"""
        Open the connection to the OTGW

        Must be overridden in implementing classes. Called before reading of
        the data starts. Should not return until the connection is opened, so
        an immediately following call to `read` does not fail.
        """
        raise NotImplementedError("Abstract method")

    def close(self):
        r"""
        Close the connection to the OTGW

        Must be overridden in implementing classes. Called after reading of
        the data is finished. Should not return until the connection is closed.
        """
        raise NotImplementedError("Abstract method")

    def write(self, data):
        r"""
        Write data to the OTGW

        Must be overridden in implementing classes. Called when a command is
        received that should be sent to the OTGW. Should pass on the data
        as-is, not appending line feeds, carriage returns or anything.
        """
        raise NotImplementedError("Abstract method")

    def read(self, timeout):
        r"""
        Read data from the OTGW

        Must be overridden in implementing classes. Called in a loop while the
        client is running. May return any block of data read from the
        connection, be it line by line or any other block size. Must return a
        string. Line feeds and carriage returns should be passed on unchanged.
        Should adhere to the timeout passed. If only part of a data block is
        read before the timeout passes, return only the part that was read
        successfully, even if it is an empty string.
        """
        raise NotImplementedError("Abstract method")

    def join(self):
        r"""
        Block until the worker thread finishes or exit signal received
        """
        try:
            while self._worker_thread.isAlive():
                self._worker_thread.join(1)
        except SignalExit:
            self.stop()
        except SignalAlarm:
            self.reconnect()

    def start(self):
        r"""
        Connect to the OTGW and start reading data
        """
        if self._worker_thread:
            raise RuntimeError("Already running")
        self._worker_thread = Thread(target=self._worker)
        self._worker_thread.start()
        log.info("Started worker thread #%s", self._worker_thread.ident)

    def stop(self):
        r"""
        Stop reading data and disconnect from the OTGW
        """
        if not self._worker_thread:
            raise RuntimeError("Not running")
        log.info("Stopping worker thread #%s", self._worker_thread.ident)
        self._worker_running = False
        self._worker_thread.join()

    def reconnect(self, reconnect_pause=10):
        r"""
        Attempt to reconnect when the connection is lost
        """
        try:
            self.close()
        except Exception:
            pass

        while self._worker_running:
            try:
                self.open()
                self._listener(('', 'online')) # this goes to the main pub topic, topic is prefixed in on_otgw_message
                break
            except Exception:
                self._listener(('', 'offline')) # this goes to the main pub topic, topic is prefixed in on_otgw_message
                log.warning("Waiting %d seconds before retrying", reconnect_pause)
                sleep(reconnect_pause)

    def mqtt_to_otgw(self, mqtt_message):
        # message = {topic, payload}


        # Find the correct command generator from the dict above
        command_generator = command_generators.get(mqtt_message.topic)
        if command_generator:
            # Get the command and send it to the OTGW

            # Get the command and send it to the OTGW
            if self._command_feedback_required:
                now = int(time())
                # If the topic exists in the stored messages dict, and is unchanged, don't send out message
                if mqtt_message.topic in self.stored_commands:
                    log.error("inside loop. {} - {}".format(self.stored_commands[mqtt_message.topic] , mqtt_message.payload))
                    if self.stored_commands[mqtt_message.topic]['data'] == mqtt_message.payload:
                        #update with new request date
                        #reset counter
                        log.error("reset count")
                        self.stored_commands[mqtt_message.topic]['count'] = 0
                        return
                    # else:
                        # log.error("update with new data")
                        # self.stored_commands[mqtt_message.topic] = {'received': now, 'data': mqtt_message.payload}
                
                # save new command to stored messages dict
                
                self.stored_commands[mqtt_message.topic] = {'received': now, 'data': mqtt_message.payload, 'count': 0}

            log.error(mqtt_message.payload.decode('ascii', 'ignore'))
            command = command_generator(mqtt_message.payload.decode('ascii', 'ignore'))
            log.error("Sending command: '{}'".format(command))
            self.send("{}\r".format(command))
            #
            # implement check on incoming msg if we have command confirmation. use new topic_response dict
            #

        else:
            log.error("No command_generator found for topic: {}".format(mqtt_message.topic))

    def send(self, data):
        self._send_buffer.append(data)

    def check_command_confirmation_queue(self, msg):
        
        topic, payload = msg
        # log.error( "topic: {}, payload: {}".format(topic, payload))
        if self.IN_CONFIRMATION: 
            log.error("loop protection")
            return
        try:
            self.IN_CONFIRMATION = True


            # if we get a response, pop from list
            if ('/command_response') in topic:
                log.error("command_response msg: {}".format(msg))
                # strip /command_response 
                cr_topic = topic.replace('/command_response','')

                if cr_topic in self.stored_commands:

                    key_val = self.stored_commands[cr_topic]['data']
                    payload = str(payload).encode('utf8')
                    # log.error("values: topic {} -- msg {}".format(key_val, payload))
                    if (key_val == payload):
                        log.error("popping '{}' from queue".format(cr_topic))
                        self.stored_commands.pop(cr_topic)
                
            # loop queue, populate keys var keys that are invalid or exceeded max retries
            now = int(time())
            keys=[] 
            response=[]
            err_code = 0
            for topic, cmd in self.stored_commands.items():
                log.debug("diff: {}".format(now - cmd['received']))

                # if we're in this loop, at minimum we have stored commands
                err_code = 1
                command_generator = command_generators.get(topic)

                if not command_generator:
                    # no generator or too many tries, remove from queue
                    log.error("No generator found for command: {} - {}". format(topic, cmd))
                    keys.append(topic)
                    err_code = 2

                elif cmd['count'] > self._command_max_retries:
                    log.error("too many tries for command: {} - {}". format(topic, cmd))
                    keys.append(topic)
                    err_code = 3

                # if older than command_timeout, process
                elif now - cmd['received'] > self._command_timeout:
                    # throw command error
                    log.error("Did not receive feedback on command {} -- {} within the timeout".format(topic, cmd['data']))
                    err_code = 4

                    # resend command
                    cmd['count']+=1
                    cmd['received'] = now
                    command = command_generator(cmd['data'])
                    log.error("re-sending command: '{}'".format(command))
                    self.send("{}\r".format(command))

                # else:
                #     log.error("should never reach this!") 

            # purge invalid or max retries keys
            for x in keys:
                log.error("deleting : {}".format(x))
                del self.stored_commands[x]

            # reset error status
            response.append(('/command_response_error/state', err_code))
            self.IN_CONFIRMATION = False                

            return response 

        except Exception as e:
            log.error("error - {}".format(e))
            self.IN_CONFIRMATION = False
            return None

    def _worker(self):
        # _worker_running should be True while the worker is running
        self._worker_running = True

        try:
          # Open the connection to the OTGW
           self.open()
        except ConnectionException:
           log.warning("Retrying immediately")
           self.reconnect()

        # Compile a regex that will only match the first part of a string, up
        # to and including the first time a line break and/or carriage return
        # occurs. Match any number of line breaks and/or carriage returns that
        # immediately follow as well (effectively discarding empty lines)
        line_splitter = re.compile(r'^.*[\r\n]+')

        # Create a buffer for read data
        data = ""

        while self._worker_running:
            log.debug("Worker run with initial data: '%s'", data)
            try:
                # Send MQTT messages to TCP serial
                while self._send_buffer:
                    self.write(self._send_buffer[0])
                    self._send_buffer.popleft()
                # Receive TCP serial data for MQTT
                read = self.read(timeout=0.5)
                if read:
                    data += read
            except ConnectionException:
                self.reconnect()
            # Find all the lines in the read data

            log.debug("Retrieved data: '%s'", data)
            while True:
                m = line_splitter.match(data)
                if not m:
                    log.debug("Unable to extract line from data '%s'", data)
                    # There are no full lines yet, so we have to read some more
                    break
                log.debug("Extracted line: '%s'", m.group())

                raw_message = m.group().rstrip('\r\n')
                # Get all the messages for the line that has been read,
                # most lines will yield no messages or just one, but
                # flags-based lines may return more than one.
                log.debug("Raw message: %s", raw_message)
                for msg in get_messages(raw_message):
                    try:
                        if self._command_feedback_required:
                            # verify command queue
                            # log.error("msg: {}".format(msg))
                            err = self.check_command_confirmation_queue(msg)
                            if err:
                                for err_msg in err:
                                    self._listener(err_msg)
                        # Pass each message on to the listener
                        log.debug("Execute message: '%s'", msg)
                        self._listener(msg)
                    except Exception as e:
                        # Log a warning when an exception occurs in the
                        # listener
                        log.exception("Error in listener handling for message '%s', jump to close and reconnect: %s", raw_message, str(e))

                # Strip the consumed line from the buffer
                data = data[m.end():]
                log.debug("Left data: '%s'", data)

        # After the read loop, close the connection and clean up
        self.close()
        self._worker_thread = None

class ConnectionException(Exception):
    pass

class SignalExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass

class SignalAlarm(Exception):
    """
    Custom exception upon trigger of SIGALRM signal
    """
    pass
