#!/usr/bin/env bash

CAMEL_PATH=''
#TSHARK_FILTER="(camel.eventTypeBCSM == 2 and camel.eventTypeBCSM == 17) or \
#               (camel.eventTypeBCSM == 2 and camel.eventTypeBCSM == 9) and \
#               camel.local == 44"

TSHARK_FILTER='(camel.eventTypeBCSM==2 and camel.serviceKey == 2 and camel.local == 0 and not camel.eventTypeSMS)'
OUTPUT_PATH=''

# print help, error string and exit
# Globals:
#   None
# Arguments:
#   $1 - error string
# Returns:
#   None
function print_help() {
    [[ -n "${1}" ]] && echo -ne "${1}\n"
    echo -ne 'Usage is:\n'
    echo -ne '--camel-path= path to CAMEL pcap raw log files\n\n'
    echo -ne '--output-path= path where to write output CSV files, default tmp\n\n'
    exit 1
}

# parse CLI arguments
# Globals:
#   CAMEL_PATH
# Arguments:
#   $@ - CLI arguments
# Returns:
#   None
function parse_command_line_arg() {
    while [ $# -gt 0 ]
    do
        case $1 in
            --help )
            print_help
            ;;

            --camel-path=*)
            param=$( echo "${1}" | sed -e 's/^[^=]*=//g' )
            [[ "X" = "X${param}" ]] && print_help "invalid camel-path!"
            CAMEL_PATH="${param}"
            ;;

            --output-path=*)
            param=$( echo "${1}" | sed -e 's/^[^=]*=//g' )
            [[ "X" = "X${param}" ]] && echo "output-path is not set,
                                             using default /tmp!"
            OUTPUT_PATH="${param:-/tmp}"
            ;;

            *)
            # no more options, get on with life
            print_help "Unknown parameter ${1}. Exiting."
            ;;

        esac
        shift         # otherwise if no-match not shifted, would cause infinite loop
    done
}

# parse CAMEL pcap dump files
# Globals:
#   CAMEL_PATH
#   OUTPUT_PATH
# Arguments:
#   None
# Returns:
#   None
function parse_camel() {
    local file
    local file_bn
    local camel_raw_files=$(ls ${CAMEL_PATH}/*.pcap)
    [[ -z "${camel_raw_files}" ]] && { echo "WARNING: There is no pcap \
                                       files under ${CAMEL_PATH}"; exit 3;
                                     }
    for file in ${camel_raw_files} ; do
        file_bn=$(basename "${file}")
#        tshark -r "${file}" -Y "${TSHARK_FILTER}" -n -T fields -E separator=, -e frame.time_epoch \
#                                                                              -e e164.calling_party_number.digits \
#                                                                              -e e164.called_party_number.digits \
#                                                                              -e gsm_a.dtap.cld_party_bcd_num \
#                                                                              -e camel.callReferenceNumber \
#                                                                              -e camel.callAttemptElapsedTimeValue \
#                                                                              -e camel.releaseCauseValue > "${OUTPUT_PATH}/${file_bn}.txt"
        tshark -r "${file}" -Y "${TSHARK_FILTER}" -n -T fields -E separator=, -e frame.time_epoch \
                                                                              -e e164.calling_party_number.digits \
                                                                              -e gsm_a.dtap.cld_party_bcd_num \
                                                                              -e camel.eventTypeBCSM \
                                                                              -e camel.serviceKey \
                                                                              -e camel.ext_basicServiceCode > "${OUTPUT_PATH}/${file_bn}.txt"
        db_upload "${OUTPUT_PATH}/${file_bn}.txt"
    done
}

# upload parsed logs into DB
# Globals:
#   None
# Arguments:
#   $1 - file which should be uploaded to DB
# Returns:
#   None
function db_upload() {
    local file="$1"
    python camel_parser.py "${file}"
}

parse_command_line_arg "$@"

[[ -z "${CAMEL_PATH}" ]] && print_help "camel-path is not set"

parse_camel
