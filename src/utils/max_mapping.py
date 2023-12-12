
#Will take from the database
mappingJson = {
  "1500":{"line_val":"{1500}{config_1500}","line_replace":"config_1500"},
  "1510":{"line_val":"{1510}{line_1510}","line_replace":"line_1510"},
  "1520":{"line_val":"{1520}{settlement_date}","line_replace":"settlement_date"},
  "2000":{"line_val":"{2000}{settlement_amount}","line_replace":"settlement_amount"},
  "3100":{"line_val":"{3100}{config_3100}","line_replace":"config_3100"},
  "3320":{"line_val":"{3320}{instruction_id}{replace_common}","line_replace":"instruction_id,replace_common"},
  "3400":{"line_val":"{3400}{line_3400_start}","line_replace":"line_3400_start","rule":"rule_2"},
  "3600":{"line_val":"{3600}{bus_fn_code}","line_replace":"bus_fn_code","rule":"rule_10"},
  "4000":{"line_val":"{4000}{line_4000_start}{replace_common}{intermediary_agent_name}{replace_common}{intermediary_agent_address_1}{replace_common}{intermediary_agent_address_2}{replace_common}{intermediary_agent_address_3}{replace_common}"
          ,"line_replace":"line_4000_start,intermediary_agent_name,intermediary_agent_address_1,intermediary_agent_address_2,intermediary_agent_address_3,replace_common","rule":"rule_3"},
  "4100":{"line_val":"{4100}{line_4100_start}{replace_common}{creditor_agent_name}{replace_common}{creditor_agent_address_1}{replace_common}{creditor_agent_address_2}{replace_common}{creditor_agent_address_3}{replace_common}"
          ,"line_replace":"line_4100_start,creditor_agent_name,creditor_agent_address_1,creditor_agent_address_2,creditor_agent_address_3,replace_common","rule":"rule_4","rule":"rule_4"},
  "4200":{"line_val":"{4200}{line_4200_start}{replace_common}{creditor_name}{replace_common}{creditor_address_1}{replace_common}{creditor_address_2}{replace_common}{creditor_address_3}{replace_common}"
          ,"line_replace":"line_4200_start,creditor_name,creditor_address_1,creditor_address_2,creditor_address_3,replace_common","rule":"rule_5"},
  "4320":{"line_val":"{4320}{end_to_end_id}","line_replace":"end_to_end_id","rule":"rule_11"},
  "4400":{"line_val":"{4400}{line_4400_start}{ultimate_debtor_account_id}{replace_common}{ultimate_debtor}{replace_common}{ultimate_debtor_address_1}{replace_common}{ultimate_debtor_address_2}{replace_common}{ultimate_debtor_address_3}{replace_common}"
          ,"line_replace":"line_4400_start,ultimate_debtor_account_id,ultimate_debtor,ultimate_debtor_address_1,ultimate_debtor_address_2,ultimate_debtor_address_3,replace_common","rule":"rule_6"},
  "5000":{"line_val":"{5000}{config_5000}{debtor_account}{replace_common}{debtor_name}{replace_common}{debtor_address_1}{replace_common}{debtor_address_2}{replace_common}{debtor_address_3}{replace_common}"
          ,"line_replace":"config_5000,debtor_account,debtor_name,debtor_address_1,debtor_address_2,debtor_address_3,replace_common"},
  "5100":{"line_val":"{5100}{line_5100_start}{replace_common}{debtor_agent_name}{replace_common}{debtor_agent_address_1}{replace_common}{debtor_agent_address_2}{replace_common}{debtor_agent_address_3}{replace_common}"
          ,"line_replace":"line_5100_start,debtor_agent_name,debtor_agent_address_1,debtor_agent_address_2,debtor_agent_address_3,replace_common","rule":"rule_7"},
  "5200":{"line_val":"{5200}{line_5200_start}{replace_common}{previous_instructing_agent_name}{replace_common}{previous_instructing_agent_address_1}{replace_common}{previous_instructing_agent_address_2}{replace_common}{previous_instructing_agent_address_3}{replace_common}"
          ,"line_replace":"line_5200_start,previous_instructing_agent_name,previous_instructing_agent_address_1,previous_instructing_agent_address_2,previous_instructing_agent_address_3,replace_common","rule":"rule_8"},
  "5400":{"line_val":"{5400}{line_5400_start}","line_replace":"5400_start","rule":"rule_9"},
  "6000":{"line_val":"{6000}{obi_1}{replace_common}{obi_2}{replace_common}{obi_3}{replace_common}{obi_4}{replace_common}",
          "line_replace":"obi_1,obi_2,obi_3,obi_4,replace_common","rule":"rule_12"},
  "6500":{"line_val":"{6500}{bbi_info_1}{replace_common}{bbi_info_2}{replace_common}{bbi_info_3}{replace_common}{bbi_info_4}{replace_common}{bbi_info_5}{replace_common}{bbi_info_6}{replace_common}",
          "line_replace":"bbi_info_1,bbi_info_2,bbi_info_3,bbi_info_4,bbi_info_5,bbi_info_6,replace_common","rule":"rule_13"},
}