import Config

class rules:
    def __init__(self):
        print("Rules")
        self.comp_val = Config.properties.comp_val

    def rule_10(self,each_tran):
        if "bus_fn_code" in each_tran and each_tran["bus_fn_code"] != self.comp_val:
            if each_tran["bus_fn_code"] not in "BTR,CTR,CTP,DRW":
                raise("Rule 10(3600) did not match and erroring out the record")
        else:
            raise("Rule 10(3600) did not match and erroring out the record")
        return False, each_tran

    def rule_3(self,each_tran):
        if each_tran["intermediary_agent_bic"] !=  self.comp_val and each_tran["intermediary_agent_id"] ==  self.comp_val and each_tran["intermediary_agent_account"] ==  self.comp_val :
            vall = "B" + each_tran["intermediary_agent_bic"]
        elif each_tran["intermediary_agent_bic"] !=  self.comp_val and each_tran["intermediary_agent_account"] !=  self.comp_val :
            vall = "D" + each_tran["intermediary_agent_account"] +Config.properties.replace_common + each_tran["intermediary_agent_bic"]
            each_tran['intermediary_agent_address_3'] = ""
        elif each_tran["intermediary_agent_bic"] ==  self.comp_val and each_tran["intermediary_agent_id"] !=  self.comp_val and each_tran["intermediary_agent_account"] ==  self.comp_val :
            vall = "F" + each_tran["intermediary_agent_id"]
        elif each_tran["intermediary_agent_bic"] ==  self.comp_val and each_tran["intermediary_agent_id"] ==  self.comp_val and each_tran["intermediary_agent_account"] !=  self.comp_val :
            vall = "D" + each_tran["intermediary_agent_account"]
        elif each_tran["intermediary_agent_bic"] ==  self.comp_val and each_tran["intermediary_agent_id"] ==  self.comp_val and each_tran["intermediary_agent_account"] ==  self.comp_val and each_tran["intermediary_agent_name"] != self.comp_val:
            vall = ""
        elif each_tran["intermediary_agent_bic"] ==  self.comp_val and each_tran["intermediary_agent_id"] ==  self.comp_val and each_tran["intermediary_agent_account"] ==  self.comp_val and each_tran["intermediary_agent_name"] == self.comp_val and each_tran["intermediary_agent_address_1"] == self.comp_val:
            return True, each_tran
        else:
            raise("Rule 3(4000) did not match and erroring out the record")
        each_tran["line_4000_start"] = vall
        return False,each_tran
    
    def rule_4(self,each_tran):
        if each_tran["creditor_agent_bic"] !=  self.comp_val and each_tran["creditor_agent_id"] ==  self.comp_val and each_tran["creditor_agent_account"] ==  self.comp_val :
            vall = "B" + each_tran["creditor_agent_bic"]
        elif each_tran["creditor_agent_bic"] !=  self.comp_val and each_tran["creditor_agent_account"] !=  self.comp_val :
            vall = "D" + each_tran["creditor_agent_account"] +Config.properties.replace_common + each_tran["creditor_agent_bic"]
            each_tran['creditor_agent_address_3'] = ""
        elif each_tran["creditor_agent_bic"] ==  self.comp_val and each_tran["creditor_agent_id"] !=  self.comp_val and each_tran["creditor_agent_account"] ==  self.comp_val :
            vall = "F" + each_tran["creditor_agent_id"]
        elif each_tran["creditor_agent_bic"] ==  self.comp_val and each_tran["creditor_agent_id"] ==  self.comp_val and each_tran["creditor_agent_account"] !=  self.comp_val :
            vall = "D" + each_tran["creditor_agent_account"]
        elif each_tran["creditor_agent_bic"] ==  self.comp_val and each_tran["creditor_agent_id"] ==  self.comp_val and each_tran["creditor_agent_account"] ==  self.comp_val and each_tran["creditor_agent_name"] != self.comp_val:
            vall = ""
        elif each_tran["creditor_agent_bic"] ==  self.comp_val and each_tran["creditor_agent_id"] ==  self.comp_val and each_tran["creditor_agent_account"] ==  self.comp_val and each_tran["creditor_agent_name"] == self.comp_val and each_tran["creditor_agent_address_1"] == self.comp_val:
            return True, each_tran
        else:
            raise("Rule 4(4100) did not match and erroring out the record")
        each_tran["line_4100_start"] = vall
        return False,each_tran
    
    def rule_5(self,each_tran):
        if each_tran["creditor_bic"] !=  self.comp_val and each_tran["creditor_id"] ==  self.comp_val and each_tran["creditor_account"] ==  self.comp_val :
            vall = "B" + each_tran["creditor_bic"]
        elif each_tran["creditor_bic"] !=  self.comp_val and each_tran["creditor_account"] !=  self.comp_val :
            vall = "D" + each_tran["creditor_account"] +Config.properties.replace_common + each_tran["creditor_bic"]
            each_tran['creditor_address_3'] = ""
        elif each_tran["creditor_bic"] ==  self.comp_val and each_tran["creditor_id"] !=  self.comp_val and each_tran["creditor_account"] ==  self.comp_val :
            vall = "F" + each_tran["creditor_id"]
        elif each_tran["creditor_bic"] ==  self.comp_val and each_tran["creditor_id"] ==  self.comp_val and each_tran["creditor_account"] !=  self.comp_val :
            vall = "D" + each_tran["creditor_account"]
        elif each_tran["creditor_bic"] ==  self.comp_val and each_tran["creditor_id"] ==  self.comp_val and each_tran["creditor_account"] ==  self.comp_val and each_tran["creditor_name"] != self.comp_val:
            vall = ""
        elif each_tran["creditor_bic"] ==  self.comp_val and each_tran["creditor_id"] ==  self.comp_val and each_tran["creditor_account"] ==  self.comp_val and each_tran["creditor_name"] == self.comp_val and each_tran["creditor_address_1"] == self.comp_val:
            return True, each_tran
        else:
            raise("Rule 5(4200) did not match and erroring out the record")
        each_tran["line_4200_start"] = vall
        return False,each_tran
    
    def rule_6(self,each_tran):
        if each_tran["msg_sub_type"] ==  "32" and each_tran["ultimate_debtor_account_id"] !=  self.comp_val:
            each_tran["line_4400_start"] = "D"
        elif each_tran["msg_sub_type"] !=  "32":
            return True,each_tran
        return False,each_tran

    def rule_7(self,each_tran):
        if each_tran["debtor_agent_bic"] !=  self.comp_val and each_tran["debtor_agent_id"] ==  self.comp_val and each_tran["debtor_agent_account"] ==  self.comp_val :
            vall = "B" + each_tran["debtor_agent_bic"]
        elif each_tran["debtor_agent_bic"] !=  self.comp_val and each_tran["debtor_agent_account"] !=  self.comp_val :
            vall = "D" + each_tran["debtor_agent_account"] +Config.properties.replace_common + each_tran["debtor_agent_bic"]
            each_tran['debtor_agent_address_3'] = ""
        elif each_tran["debtor_agent_bic"] ==  self.comp_val and each_tran["debtor_agent_id"] !=  self.comp_val and each_tran["debtor_agent_account"] ==  self.comp_val :
            vall = "F" + each_tran["debtor_agent_id"]
        elif each_tran["debtor_agent_bic"] ==  self.comp_val and each_tran["debtor_agent_id"] ==  self.comp_val and each_tran["debtor_agent_account"] !=  self.comp_val :
            vall = "D" + each_tran["debtor_agent_account"]
        elif each_tran["debtor_agent_bic"] ==  self.comp_val and each_tran["debtor_agent_id"] ==  self.comp_val and each_tran["debtor_agent_account"] ==  self.comp_val and each_tran["debtor_agent_name"] != self.comp_val and (each_tran["debtor_agent_address_1"] != self.comp_val or each_tran["debtor_agent_address_2"] != self.comp_val or each_tran["debtor_agent_address_3"] != self.comp_val):
            vall = ""
        elif each_tran["debtor_agent_bic"] ==  self.comp_val and each_tran["debtor_agent_id"] ==  self.comp_val and each_tran["debtor_agent_account"] ==  self.comp_val and each_tran["debtor_agent_name"] == self.comp_val and each_tran["debtor_agent_address_1"] == self.comp_val and each_tran["debtor_agent_address_2"] == self.comp_val and each_tran["debtor_agent_address_3"] == self.comp_val:
            return True, each_tran
        else:
            raise("Rule 7(5100) did not match and erroring out the record")
        each_tran["line_5100_start"] = vall
        return False,each_tran
    
    def rule_8(self,each_tran):
        if each_tran["previous_instructing_agent_bic"] !=  self.comp_val and each_tran["previous_instructing_agent_id"] ==  self.comp_val and each_tran["previous_instructing_agent_account"] ==  self.comp_val :
            vall = "B" + each_tran["previous_instructing_agent_bic"]
        elif each_tran["previous_instructing_agent_bic"] !=  self.comp_val and each_tran["previous_instructing_agent_account"] !=  self.comp_val :
            vall = "D" + each_tran["previous_instructing_agent_account"] +Config.properties.replace_common + each_tran["previous_instructing_agent_bic"]
            each_tran['previous_instructing_agent_address_3'] = ""
        elif each_tran["previous_instructing_agent_bic"] ==  self.comp_val and each_tran["previous_instructing_agent_id"] !=  self.comp_val and each_tran["previous_instructing_agent_account"] ==  self.comp_val :
            vall = "F" + each_tran["previous_instructing_agent_id"]
        elif each_tran["previous_instructing_agent_bic"] ==  self.comp_val and each_tran["previous_instructing_agent_id"] ==  self.comp_val and each_tran["previous_instructing_agent_account"] !=  self.comp_val :
            vall = "D" + each_tran["debtor_agent_account"]
        elif each_tran["previous_instructing_agent_bic"] ==  self.comp_val and each_tran["previous_instructing_agent_id"] ==  self.comp_val and each_tran["previous_instructing_agent_account"] ==  self.comp_val and each_tran["previous_instructing_agent_name"] != self.comp_val and (each_tran["previous_instructing_agent_address_1"] != self.comp_val or each_tran["previous_instructing_agent_address_2"] != self.comp_val or each_tran["previous_instructing_agent_address_3"] != self.comp_val):
            vall = ""
        elif each_tran["previous_instructing_agent_bic"] ==  self.comp_val and each_tran["previous_instructing_agent_id"] ==  self.comp_val and each_tran["previous_instructingr_agent_account"] ==  self.comp_val and each_tran["previous_instructing_agent_name"] == self.comp_val and each_tran["previous_instructing_agent_address_1"] == self.comp_val and each_tran["previous_instructing_agent_address_2"] == self.comp_val and each_tran["previous_instructing_agent_address_3"] == self.comp_val:
            return True, each_tran
        else:
            raise("Rule 8(5200) did not match and erroring out the record")
        each_tran["line_5200_start"] = vall
        return False,each_tran
    
    def rule_9(self,each_tran):
        if each_tran["msg_sub_type"] ==  "32" and each_tran["ultimate_creditor_clearing_id"] !=  self.comp_val:
            each_tran["line_5400_start"] = each_tran["ultimate_creditor_clearing_id"] 
        elif each_tran["msg_sub_type"] !=  "32":
            return True,each_tran
        return False,each_tran

    def rule_2(self,each_tran):
        vall = ""
        if each_tran["instructed_agent_id"] !=  self.comp_val :
            value = str(int(each_tran["instructed_agent_id"]))
            if len(value) < 9:
                value = "0" * (9 - len(value) ) + value
            each_tran["instructed_agent_id"] =  value
            length = len(str(each_tran["instructed_agent_name"]))
            if length >= 18:
                length = 18
            vall = each_tran["instructed_agent_id"] + str(each_tran["instructed_agent_name"])[0:length]
        elif each_tran["intermediary_agent_id"] !=  self.comp_val :
            value = str(int(each_tran["intermediary_agent_id"]))
            if len(value) < 9:
                value = "0" * (9 - len(value) ) + value
            each_tran["intermediary_agent_id"] =  value
            length = len(str(each_tran["intermediary_agent_name"]))
            if length >= 18:
                length = 18
            vall = each_tran["intermediary_agent_id"] + each_tran["intermediary_agent_name"][0:length]
        elif each_tran["creditor_agent_id"] !=  self.comp_val :
            value = str(int(each_tran["creditor_agent_id"]))
            if len(value) < 9:
                value = "0" * (9 - len(value) ) + value
            length = len(str(each_tran["creditor_agent_name"]))
            if length >= 18:
                length = 18
            vall = value + each_tran["creditor_agent_name"][0:length]
        elif each_tran["DBT MOP"] ==  "BOOK" and each_tran["CDT MOP"] ==  "BOOK" : 
            vall = str(Config.properties.config_3100)
        else:
            raise("Rule 2(3400) did not match and erroring out the record")
        print(vall)
        each_tran["line_3400_start"] = vall
        return False,each_tran

    def rule_11(self,each_tran):
        if each_tran["end_to_end_id"] ==  self.comp_val:
            return True,each_tran
        else:
            return False,each_tran
    
    def rule_12(self,each_tran):
        if each_tran["obi_1"] ==  self.comp_val and each_tran["obi_2"] ==  self.comp_val and each_tran["obi_3"] ==  self.comp_val and each_tran["obi_4"] ==  self.comp_val :
            return True,each_tran
        else:
            return False,each_tran
    
    def rule_13(self,each_tran):
        if each_tran["bbi_info_1"] ==  self.comp_val and each_tran["bbi_info_2"] ==  self.comp_val and each_tran["bbi_info_3"] ==  self.comp_val and each_tran["bbi_info_4"] ==  self.comp_val and each_tran["bbi_info_5"] ==  self.comp_val and each_tran["bbi_info_6"] ==  self.comp_val :
            return True,each_tran
        else:
            return False,each_tran
    