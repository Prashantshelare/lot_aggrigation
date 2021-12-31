# from django.shortcuts import render
from authentication.validator import APIKeyValidator
from rest_framework.decorators import authentication_classes
from rest_framework.views import APIView
from django.http.response import JsonResponse
from datetime import datetime
import time
import json
import mysql.connector
from itertools import combinations

import pandas as pd
import os
# import traceback
import re


# import json


# Create your views here.

def make_error_response(error, message, status_code, url):
    resp = {"body": {
        "path": url,
        "success": "false",
        "error": error,
        "message": message,
    }, "status_code": status_code}
    return resp


class Lot_Aggregation(APIView):
    @authentication_classes(APIKeyValidator)
    def post(self, request):
        dbConn = mysql.connector.connect(
            host=os.environ["DBHOST"],
            port=os.environ["DBPORT"],
            user=os.environ["DBUSER"],
            password=os.environ["DBPASS"],
            auth_plugin='mysql_native_password',
        )
        dbCursor = dbConn.cursor()
        if "apiKey" in request.query_params:
            query = f"""SELECT count(*) as `found` FROM cdt_master_data.drkrishi_source where ApiKey='{request.query_params["apiKey"]}' and Name = "Agriota";"""
            # exc = dbCursor.execute(query)
            dbCursor.execute(query)
            api_key_verify = dbCursor.fetchone()
            if api_key_verify[0] > 0:
                # print('len(request.data) =', len(request.data))
                if len(request.data) == 0:
                    resp = make_error_response(error="RGTERR-003", message="Required request body is missing",
                                               status_code=417, url=request.build_absolute_uri())
                else:
                    if len(request.data) < 1:  # When it is possible
                        resp = make_error_response(error="RGTERR-003", message="Required request body is missing",
                                                   status_code=417, url=request.build_absolute_uri())
                    else:
                        # https://api.cropdatadev.tk/lot-aggregation/v1.0/?apiKey=yourApiKey
                        agf_tag = 0
                        input_json = request.data
                        print("Input =", input_json)
                        print("type =", type(input_json))
                        startTime = time.clock()
                        if "lot_details" in input_json:
                            # print("YES")
                            agf_tag = 1
                            data = pd.DataFrame(input_json["rights"])
                            lot_upper_threshold = input_json["lot_details"]["lot_upper_threshold"]
                            lot_lower_threshold = input_json["lot_details"]["lot_lower_threshold"]
                            lot_quantity = input_json["lot_details"]["lot_quantity"]
                            lot_rights = pd.DataFrame(input_json["lot_details"]["lot_rights"])
                            # print("data =", data)
                            # print("lot_rights =", lot_rights)

                        else:
                            data = pd.json_normalize(input_json)

                        if "regionId" not in data.columns.tolist():
                            resp = make_error_response(error="RGTERR-028", message="Invalid Json format detected",
                                                       status_code=417, url=request.build_absolute_uri())
                        else:
                            obj_lot = lot_aggregation(data)
                            if agf_tag == 0:
                                result = obj_lot.lot_aggregation_algo()
                            else:
                                if lot_lower_threshold <= lot_quantity <= lot_upper_threshold:
                                    result = obj_lot.lot_agf_algo(lot_rights, lot_upper_threshold, lot_lower_threshold,
                                                                  lot_quantity)
                                else:
                                    # result = "Lot_Size_Details are not Correct"
                                    resp = make_error_response(error="RGTERR-030",
                                                               message="Lot_Size_Details are not Correct",
                                                               status_code=417, url=request.build_absolute_uri())
                                    return JsonResponse(resp)
                            print("result =", result)
                            print("Lot_Creation_Time =", round(time.clock() - startTime, 2), "seconds")
                            return JsonResponse(result, status=200, safe=False)
                        # except Exception as e:
                        #     # print(traceback.format_exc())
                        #     resp = make_error_response(error="RGTERR-003", message=traceback.format_exc(),
                        #                                status_code=401, url=request.build_absolute_uri())
                        # resp["body"] = traceback.format_exc()
                        # resp["status_code"] = 401

            else:
                resp = make_error_response(error="RGTERR-002", message="Invalid API Key",
                                           status_code=406, url=request.build_absolute_uri())
        else:
            resp = make_error_response(error="RGTERR-001", message="Api Key is required to access this Resource",
                                       status_code=406, url=request.build_absolute_uri())
        return JsonResponse(resp)


class lot_aggregation:
    def __init__(self, data):
        self.dbConn = mysql.connector.connect(
            host=os.environ["DBHOST"],
            port=os.environ["DBPORT"],
            user=os.environ["DBUSER"],
            password=os.environ["DBPASS"],
            auth_plugin='mysql_native_password',
        )
        self.dbCursor = self.dbConn.cursor()
        self.errors = []
        self.data = data
        self.data["regionId"] = self.data["regionId"].astype(int)
        self.data["farmerRating"] = self.data["farmerRating"].astype(float)
        self.data["commodityId"] = self.data["commodityId"].astype(int)
        self.data["varietyId"] = self.data["varietyId"].astype(int)
        self.data.loc[self.data['harvestWeek'] == "", 'harvestWeek'] = 0
        self.data["harvestWeek"] = self.data["harvestWeek"].astype(int)
        self.data["rightId"] = self.data["rightId"].astype(str).apply(lambda x: re.sub("[^0-9]+", "", x))
        self.data["mbepValue"] = self.data["mbepValue"].astype(float)
        self.data["quantity"] = self.data["quantity"].astype(float)
        self.data["quality"] = self.data["quality"].map(
            {"Band-I": 1, "Band-II": 2, "Band-III": 3, "Band-IV": 4, "Band-V": 5})
        self.data["rating_class"] = 0
        self.data.loc[(self.data['farmerRating'] >= 1) & (self.data['farmerRating'] <= 2.5), "rating_class"] = 1
        self.data.loc[(self.data['farmerRating'] > 2.5) & (self.data['farmerRating'] <= 4), "rating_class"] = 2
        self.data.loc[(self.data['farmerRating'] > 4) & (self.data['farmerRating'] <= 6), "rating_class"] = 3
        self.data.loc[(self.data['farmerRating'] > 6) & (self.data['farmerRating'] <= 8), "rating_class"] = 4
        self.data.loc[(self.data['farmerRating'] > 8) & (self.data['farmerRating'] <= 12), "rating_class"] = 5

        # print("DATA =", self.data)

        # All Rights must be of same Region
        if len(self.data["regionId"].unique()) > 1:
            self.errors.append({
                "success": "false",
                "error": "RGTERR-004",
                "message": "All Rights must be of same Region"
            })
        elif len(self.data["commodityId"].unique()) > 1:
            self.errors.append({
                "success": "false",
                "error": "RGTERR-005",
                "message": "All Rights must be of same Commodity"
            })
        elif len(self.data["cropType"].unique()) > 1:
            self.errors.append({
                "success": "false",
                "error": "RGTERR-020",
                "message": "All Rights must be of same Crop Type"
            })
        elif self.data['rightId'].duplicated().any():
            self.errors.append({
                "success": "false",
                "error": "RGTERR-024",
                "message": "Every Right ID must be unique"
            })
        self.unique_specs_df = self.data.groupby(
            ['regionId', 'commodityId', "varietyId", 'harvestWeek', "quality",
             "rating_class", "cropType"]).size().reset_index().rename(
            columns={0: 'count'})
        self.unique_specs = self.unique_specs_df.values.tolist()
        print('unique_specs =', self.unique_specs)
        self.region_commodity_variety_list = pd.read_sql("""select varietyId , commodityId , gr.regionId
                from cdt_master_data.zonal_variety zv
                inner join cdt_master_data.geo_acz ac on ac.ID = zv.AczID
                inner join cdt_master_data.geo_region gr on gr.StateCode = ac.StateCode and gr.Status = "Active"
                where ZonalCommodityID in (
                select distinct zc.ID
                from cdt_master_data.regional_commodity rc 
                inner join cdt_master_data.zonal_commodity as zc on rc.ZonalCommodityID = zc.ID and zc.Status = "Active"
                where RegionID in (select RegionID from cdt_master_data.geo_region where Status = "Active") and rc.Status = "Active") and zv.Status = "Active"
                group by VarietyID, CommodityID, gr.RegionID;""", self.dbConn)
        # print('region_commodity_variety_list =', self.region_commodity_variety_list)
        self.region_list = list(self.region_commodity_variety_list["regionId"].unique())
        self.commodity_list = list(self.region_commodity_variety_list["commodityId"].unique())
        self.variety_list = list(self.region_commodity_variety_list["varietyId"].unique())
        self.commodity_variety_list = self.region_commodity_variety_list.groupby(
            ['commodityId', "varietyId"]).size().reset_index().rename(
            columns={0: 'count'}).drop(columns=["count"]).values.tolist()
        # print('commodity_variety_list =', self.commodity_variety_list)
        self.crop_type_list = \
            pd.read_sql("""SELECT Name FROM cdt_master_data.agri_crop_type where Status = "Active";""",
                        self.dbConn)["Name"].to_list()
        # print('crop_type_list =', self.crop_type_list)
        self.unique_specs_rchw = self.data.groupby(
            ['regionId', 'commodityId', 'harvestWeek']).size().reset_index().rename(
            columns={0: 'count'})
        # print('unique_specs_rchw =', self.unique_specs_rchw)
        # if len(self.region_commodity_variety_list.values.tolist()) > 1:
        condition = []
        for combi in self.unique_specs_rchw.itertuples():
            if combi.harvestWeek == 0:
                condition.append(
                    f"( RegionID = {combi.regionId} and CommodityId = {combi.commodityId})")
            else:
                condition.append(
                    f"( RegionID = {combi.regionId} and CommodityId = {combi.commodityId} and {combi.harvestWeek} between HarvestWeekStart AND if(HarvestWeekEnd < HarvestWeekStart,HarvestWeekEnd+52, HarvestWeekEnd))")
        self.condition_str = " or ".join(condition)
        # print(self.condition_str)
        query = f"""select rc.RegionID, zc.CommodityID, any_value(MinLotSize) as MinLotSize, any_value(MaxRigtsInLot) as MaxRigtsInLot
                    from cdt_master_data.regional_commodity rc 
                    inner join cdt_master_data.zonal_commodity zc on rc.ZonalCommodityID = zc.ID
                    where {self.condition_str} group by rc.RegionID, zc.CommodityID;"""
        # print(query)
        self.reg_com = pd.read_sql(query, self.dbConn)
        print("reg_com =", self.reg_com)
        if len(self.reg_com) == 0:
            self.errors.append({
                # "path": "http://cropdata.tk:8080/lot-aggregation/",
                "success": "false",
                "error": "RGTERR-016",
                "message": "Invalid combination of Region, Commodity and Harvest week provided."

            })

    def lot_aggregation_algo(self):
        if len(self.errors) == 0:
            self.data["error"] = self.data.apply(lambda row: self.check_validation(row), axis=1)
            # self.errors.extend(self.data[self.data.error == False].error.to_list())
            self.data = self.data[self.data.error == True]
            lot_list = []
            lotList = []
            remaining_list = []
            for unq_spc in self.unique_specs:
                min_lot_size = self.reg_com[(self.reg_com.CommodityID == unq_spc[1]) &
                                            (self.reg_com.RegionID == unq_spc[0])].MinLotSize.iloc[0]

                max_rights = self.reg_com[(self.reg_com.CommodityID == unq_spc[1]) &
                                          (self.reg_com.RegionID == unq_spc[0])].MaxRigtsInLot.iloc[0]
                # print("max_rights =", max_rights, "min_lot_size =", min_lot_size)
                lot_data = []
                current_spec_data = self.data[
                    (self.data.regionId == unq_spc[0]) & (self.data.commodityId == unq_spc[1]) &
                    (self.data.varietyId == unq_spc[2]) & (self.data.harvestWeek == unq_spc[3]) &
                    (self.data.quality == unq_spc[4]) & (self.data.rating_class == unq_spc[5])]
                # print("current_spec_data =", current_spec_data)
                quantity_list = current_spec_data.sort_values(by=["quantity"], ascending=False)
                # print("quantity_list1 =", quantity_list)
                # Rights whose quantity is greater than or equal to max_rights
                right_df = current_spec_data[(current_spec_data["quantity"] >= min_lot_size)]
                # print("right_df = ", right_df["quantity"])
                for data_1 in right_df.itertuples():
                    if lot_list.count(data_1.rightId) > 0:
                        continue

                    lot_data.append([data_1.rightId])
                    lot_list.append(data_1.rightId)
                # print("lot_list =", lot_list)
                # Rights whose quantity is less than min_lot_size
                quantity_list = quantity_list[~quantity_list["rightId"].isin(lot_list)]
                print("quantity_list2 =", quantity_list)
                for items in quantity_list.itertuples():
                    if lot_list.count(items.rightId) > 0:
                        continue
                    min_range = items.mbepValue - (items.mbepValue * 0.05)
                    max_range = items.mbepValue + (items.mbepValue * 0.05)
                    # quantity_list_1 = quantity_list[
                    #     (items.mbepValue - (items.mbepValue * 0.05) <= quantity_list.mbepValue) & (
                    #             quantity_list.mbepValue <= items.mbepValue + (items.mbepValue * 0.05))]
                    quantity_list_1 = quantity_list[(min_range <= quantity_list.mbepValue) &
                                                    (quantity_list.mbepValue <= max_range)]
                    print("quantity_list_1 =", quantity_list_1)
                    for values in quantity_list_1.itertuples():
                        list_quantity = []
                        list_right = []
                        print(sum(list_quantity), len(list_quantity))
                        if lot_list.count(values.rightId) > 0:
                            continue
                        while sum(list_quantity) < min_lot_size and len(list_quantity) <= max_rights:
                            if len(list_quantity) > 1 and sum(list_quantity) != min_lot_size and \
                                    len(quantity_list_1) == 0:
                                remaining_list.extend(list_right)
                                list_right = []
                                list_quantity = []
                                break
                            if len(list_quantity) == max_rights and sum(list_quantity) < min_lot_size:
                                remaining_list.extend(list_right)
                                break
                            if len(quantity_list_1) == 1 and len(list_quantity) == 0:
                                if values.rightId not in remaining_list:
                                    print("values.rightId =", values.rightId)
                                    remaining_list.append(values.rightId)
                                    lot_list.append(values.rightId)
                                    break
                            if values.rightId not in list_right:
                                list_quantity.append(values.quantity)
                                list_right.append(values.rightId)
                                lot_list.append(values.rightId)
                                quantity_list_1 = quantity_list_1[~quantity_list_1.rightId.isin(list_right)]
                            nest = min_lot_size - sum(list_quantity)
                            min_value_df = quantity_list_1[quantity_list_1.quantity >= nest].sort_values(
                                by=["quantity"], ascending=True)
                            if len(min_value_df) > 0:
                                min_value = min_value_df["quantity"].iloc[0]
                                right_id_min = min_value_df["rightId"].iloc[0]
                                list_quantity.append(min_value)
                                list_right.append(right_id_min)
                                lot_list.append(right_id_min)
                                quantity_list_1 = quantity_list_1[~quantity_list_1.rightId.isin(list_right)]
                            else:
                                if len(list_quantity) == 0:
                                    break
                                max_value_df = quantity_list_1.sort_values(by=["quantity"], ascending=False)
                                if len(max_value_df) > 0:
                                    max_value = max_value_df["quantity"].iloc[0]
                                    right_id_min = max_value_df["rightId"].iloc[0]
                                    list_quantity.append(max_value)
                                    list_right.append(right_id_min)
                                    lot_list.append(right_id_min)
                                    quantity_list_1 = quantity_list_1[~quantity_list_1.rightId.isin(list_right)]
                                else:
                                    remaining_list.extend(list_right)
                                    break
                            if len(list_right) > 0 and sum(list_quantity) >= min_lot_size:
                                lot_data.append(list_right)
                lotList.extend(lot_data)
        else:
            lotList = []
            remaining_list = self.data.rightId.to_list()

        result = {
            "lotList": lotList,
            "remainingRights": remaining_list,
            "errors": self.errors
        }
        return result

    def lot_agf_algo(self, lot_rights, upperlimit, lowerlimit, lot_quantity):

        lot_data = lot_rights["rightId"].values.tolist()
        print("lot_data =", lot_data)
        if len(self.errors) == 0:
            self.data["error"] = self.data.apply(lambda row: self.check_validation(row), axis=1)
            # self.errors.extend(self.data[self.data.error == False].error.to_list())
            self.data = self.data[self.data.error == True]
            # Convert Lot Details as our required format
            rights_in_lot = lot_rights
            rights_in_lot["regionId"] = rights_in_lot["regionId"].astype(int)
            rights_in_lot["farmerRating"] = rights_in_lot["farmerRating"].astype(float)
            rights_in_lot["commodityId"] = rights_in_lot["commodityId"].astype(int)
            rights_in_lot["varietyId"] = rights_in_lot["varietyId"].astype(int)
            rights_in_lot.loc[rights_in_lot['harvestWeek'] == "", 'harvestWeek'] = 0
            rights_in_lot["harvestWeek"] = rights_in_lot["harvestWeek"].astype(int)
            rights_in_lot["rightId"] = rights_in_lot["rightId"].astype(str).apply(lambda x: re.sub("[^0-9]+", "", x))
            rights_in_lot["mbepValue"] = rights_in_lot["mbepValue"].astype(float)
            rights_in_lot["quantity"] = rights_in_lot["quantity"].astype(float)
            rights_in_lot["quality"] = rights_in_lot["quality"].map(
                {"Band-I": 1, "Band-II": 2, "Band-III": 3, "Band-IV": 4, "Band-V": 5})
            rights_in_lot["rating_class"] = 0
            rights_in_lot.loc[
                (self.data['farmerRating'] >= 1) & (rights_in_lot['farmerRating'] <= 2.5), "rating_class"] = 1
            rights_in_lot.loc[
                (self.data['farmerRating'] > 2.5) & (rights_in_lot['farmerRating'] <= 4), "rating_class"] = 2
            rights_in_lot.loc[
                (self.data['farmerRating'] > 4) & (rights_in_lot['farmerRating'] <= 6), "rating_class"] = 3
            rights_in_lot.loc[
                (self.data['farmerRating'] > 6) & (rights_in_lot['farmerRating'] <= 8), "rating_class"] = 4
            rights_in_lot.loc[
                (self.data['farmerRating'] > 8) & (rights_in_lot['farmerRating'] <= 12), "rating_class"] = 5

            # Create Lot Spec
            lot_specs_df = rights_in_lot.groupby(
                ['regionId', 'commodityId', "varietyId", 'harvestWeek', "quality",
                 "rating_class", "cropType"]).sum().reset_index().rename(columns={0: 'count'})
            print("lot_spec_df = ", lot_specs_df)

            lot_specs = lot_specs_df.values.tolist()[0]
            print("lot_specs =", lot_specs)
            unique_specs_updated = []
            # This is because to maintain same structure of lot_spec and data's unique spec
            for unq_spc in self.unique_specs:
                unique_specs_updated.append(unq_spc[:-1])

            # Check lot_spec present in data's unique_spec
            if lot_specs[:7] in unique_specs_updated:

                existing_quantity = lot_specs[-1]
                # Check existing_quantity lesser than lot quantity
                if existing_quantity < lot_quantity:
                    min_lac_size = lot_quantity - existing_quantity

                    max_rights = self.reg_com[(self.reg_com.CommodityID == lot_specs[1]) &
                                              (self.reg_com.RegionID == lot_specs[0])].MaxRigtsInLot.iloc[0]

                    max_rights = max_rights - len(lot_rights)  # Validation may require
                    print("max_rights =", max_rights, "min_lac_size =", min_lac_size)

                    if max_rights > 0:
                        current_spec_data = self.data[
                            (self.data.regionId == lot_specs[0]) & (self.data.commodityId == lot_specs[1]) &
                            (self.data.varietyId == lot_specs[2]) & (self.data.harvestWeek == lot_specs[3]) &
                            (self.data.quality == lot_specs[4]) & (self.data.rating_class == lot_specs[5])]
                        # print("current_spec_data =", current_spec_data)
                        df = current_spec_data.sort_values(by=["quantity"], ascending=False)
                        print("df =", df[['rightId', 'quantity']])
                        # Rights whose quantity is equal to min_lac_size
                        exact_df = df[(df["quantity"] == min_lac_size)]
                        if len(exact_df) > 0:
                            lot_data.append(int(exact_df["rightId"].iloc[0]))
                        else:
                            if existing_quantity < lowerlimit:
                                min_range = lowerlimit - existing_quantity
                                max_range = upperlimit - existing_quantity
                                print("min =", min_range, "max =", max_range)
                                # Rights whose quantity is in given range
                                range_df = df[(min_range <= df["quantity"]) & (df["quantity"] <= max_range)]
                                # print("range_df =", range_df[["rightId", "quantity"]])
                                if len(range_df) > 0:
                                    lot_data.append(int(range_df["rightId"].iloc[0]))
                                else:
                                    if max_rights > 1:
                                        list_right = []
                                        quantity_dict = {}
                                        list_quantity = []
                                        comb = []
                                        for items in df.itertuples():
                                            if items.quantity in quantity_dict:
                                                quantity_dict[items.quantity] += [int(items.rightId)]
                                            else:
                                                quantity_dict[items.quantity] = [int(items.rightId)]
                                            list_quantity.append(items.quantity)

                                        # combinations of quantities from 2 quantity upto max_rights
                                        for n in range(2, max_rights + 1):
                                            comb.append([i for i in combinations(list_quantity, n)])

                                        combinations_list = [list(x) for xl in comb for x in xl]
                                        combinations_sumList = [sum(x) for x in combinations_list]
                                        print("combinations_sumList =", combinations_sumList)
                                        # quantities combination which serve exact min_lac_size
                                        exact = combinations_list[combinations_sumList.index(min_lac_size)] \
                                            if min_lac_size in combinations_sumList else 0
                                        if exact == 0:
                                            # quantities combination which is in given range
                                            range_list = [x for x in combinations_sumList if min_range <= x <= max_range]
                                            if len(range_list) > 0:
                                                nearest = range_list[min(range(len(range_list)),
                                                                         key=lambda x: abs(range_list[x] - min_lac_size))]
                                                exact = combinations_list[combinations_sumList.index(nearest)] \
                                                    if nearest in combinations_sumList else 0

                                        elif exact != 0:
                                            for quantity in exact:
                                                list_right.append(quantity_dict[quantity][-1])
                                                del quantity_dict[quantity][-1]
                                            lot_data.extend(list_right)
                                            print("exact =", exact, "sum =", sum(exact))
                                            print("list_right =", list_right)
                                        else:
                                            self.errors.append({
                                                "message": "Sorry we didn't get any rights against required quantity",
                                                "error": "RGTERR-034"
                                            })
                                    else:
                                        self.errors.append({
                                            "message": "Maximum Rights Exceeds",
                                            "error": "RGTERR-032"
                                        })
                            else:
                                self.errors.append({
                                    "message": "lot_lower_threshold is not Correct",
                                    "error": "RGTERR-033"
                                })
                    else:
                        self.errors.append({
                            "message": "Maximum Rights Exceeds",
                            "error": "RGTERR-032"
                        })
                else:
                    # lot_data = "Lot Details are not Correct"
                    self.errors.append({
                        "message": "Lot Quantity is not correct",
                        "error": "RGTERR-031"
                    })

        else:
            lot_data = lot_data

        result = {
            "lotData": lot_data,
            "errors": self.errors
        }
        return result

    def check_validation(self, data):
        flag = True
        if data["commodityId"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "Commodity ID must be greater than zero",
                "error": "RGTERR-021"
            })
            flag = False
        if data["commodityId"] not in self.commodity_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid CommodityId provided",
                "error": "RGTERR-014"
            })
            flag = False

        if data["varietyId"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "Variety ID must be greater than zero",
                "error": "RGTERR-010"
            })
            flag = False
        if data["varietyId"] not in self.variety_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid VarietyId provided",
                "error": "RGTERR-015"
            })
            flag = False

        if [data["commodityId"], data["varietyId"]] not in self.commodity_variety_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid combination of Commodity and Variety provided",
                "error": "RGTERR-019"
            })
            flag = False

        if data["regionId"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "Region ID must be greater than zero",
                "error": "RGTERR-009"
            })
            flag = False
        if data["regionId"] not in self.region_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid RegionId provided",
                "error": "RGTERR-013"
            })
            flag = False

        if data["quality"] not in range(1, 6, 1):
            self.errors.append({
                "id": data["rightId"],
                "message": "Quality must be in between Band-I to Band-V",
                "error": "RGTERR-006"
            })
            flag = False
        if data["cropType"] not in ["Harvested", "Warehoused"]:
            if data["harvestWeek"] not in range(1, 53, 1):
                self.errors.append({
                    "id": data["rightId"],
                    "message": "Harvest Week must be in between 1 to 52",
                    "error": "RGTERR-008"
                })
                flag = False
            query = f"""select count(*)
                                from cdt_master_data.regional_commodity rc 
                                inner join cdt_master_data.zonal_commodity zc on rc.ZonalCommodityID = zc.ID
                                where RegionID = {data.regionId} and CommodityId = {data.commodityId} and {data.harvestWeek} between HarvestWeekStart AND if(HarvestWeekEnd < HarvestWeekStart,HarvestWeekEnd+52,HarvestWeekEnd);"""
            self.dbCursor.execute(query)
            harvest_week_verify = self.dbCursor.fetchone()
            if harvest_week_verify[0] == 0:
                self.errors.append({
                    "id": data["rightId"],
                    "message": "Incorrect Harvest Week for the given combination of Region and Commodity",
                    "error": "RGTERR-017"
                })
                flag = False

        if data["mbepValue"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "MBEP Value must be greater than zero",
                "error": "RGTERR-011"
            })
            flag = False

        if data["farmerRating"] not in range(1, 13, 1):
            self.errors.append({
                "id": data["rightId"],
                "message": "Farmer Rating must be in between 1 to 12",
                "error": "RGTERR-007"
            })
            flag = False

        if data["quantity"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "Quantity Value must be greater than zero",
                "error": "RGTERR-012"
            })
            flag = False

        if data["cropType"] not in self.crop_type_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid CropType provided",
                "error": "RGTERR-018"
            })
            flag = False

        # if type(data["rightId"]) is str:
        #     self.errors.append({
        #         "id": data["rightId"],
        #         "message": "Right ID should not be string",
        #         "error": "RGTERR-027"
        #     })
        #     flag = False
        if type(data["rightId"]) is float:
            self.errors.append({
                "id": data["rightId"],
                "message": "Right ID should not be Float",
                "error": "RGTERR-029"
            })
        # else:
        #     if data["rightId"] <= 1:
        #         self.errors.append({
        #             "id": data["rightId"],
        #             "message": "Right ID must be greater than zero",
        #             "error": "RGTERR-022"
        #         })
        #         flag = False
        if len(str(data["rightId"])) != 19:
            self.errors.append({
                "id": data["rightId"],
                "message": "Right ID must be of 19 digits",
                "error": "RGTERR-023"
            })
            flag = False

        if not flag:
            return False
        else:
            return True


class Lot_aggregation_manual(APIView):
    @authentication_classes(APIKeyValidator)
    def post(self, request):
        dbConn = mysql.connector.connect(
            host=os.environ["DBHOST"],
            port=os.environ["DBPORT"],
            user=os.environ["DBUSER"],
            password=os.environ["DBPASS"],
            auth_plugin='mysql_native_password',
        )
        dbCursor = dbConn.cursor()
        if "apiKey" in request.query_params:
            query = f"""SELECT count(*) as `found` FROM cdt_master_data.drkrishi_source where ApiKey='{request.query_params["apiKey"]}' and Name = "Agriota";"""
            exc = dbCursor.execute(query)
            api_key_verify = dbCursor.fetchone()
            if api_key_verify[0] > 0:
                if len(request.data) == 0:
                    resp = make_error_response(error="RGTERR-003", message="Required request body is missing",
                                               status_code=417, url=request.build_absolute_uri())
                    print(resp)
                else:
                    if len(request.data) < 1:
                        resp = make_error_response(error="RGTERR-003", message="Required request body is missing",
                                                   status_code=417, url=request.build_absolute_uri())
                    else:
                        # https://api.cropdatadev.tk/lot-aggregation/v1.0/?apiKey=yourApiKey
                        input_json = request.data
                        if "rightList" not in input_json or "relaxation" not in input_json:
                            resp = make_error_response(error="RGTERR-026",
                                                       message="Required request body should contain relaxation and rightList",
                                                       status_code=417, url=request.build_absolute_uri())
                        startTime = datetime.now()
                        # try:
                        obj_lot = lot_aggregation_manual(input_json)
                        result = obj_lot.lot_aggregation_algo()
                        return JsonResponse(result, status=200, safe=False)
                        # except Exception as e:
                        #     # print(traceback.format_exc())
                        #     resp = make_error_response(error="RGTERR-003", message=traceback.format_exc(),
                        #                                status_code=400, url=request.build_absolute_uri())
                        # print(datetime.now() - startTime)
            else:
                resp = make_error_response(error="RGTERR-002", message="Invalid API Key",
                                           status_code=406, url=request.build_absolute_uri())
        else:
            resp = make_error_response(error="RGTERR-001", message="Api Key is required to access this Resource",
                                       status_code=406, url=request.build_absolute_uri())
        return JsonResponse(resp)


class lot_aggregation_manual:
    def __init__(self, input_json):
        self.dbConn = mysql.connector.connect(
            host=os.environ["DBHOST"],
            port=os.environ["DBPORT"],
            user=os.environ["DBUSER"],
            password=os.environ["DBPASS"],
            auth_plugin='mysql_native_password',
        )
        self.dbCursor = self.dbConn.cursor()
        self.errors = []
        self.data = pd.json_normalize(input_json["rightList"])
        self.data["regionId"] = self.data["regionId"].astype(int)
        self.data["farmerRating"] = self.data["farmerRating"].astype(float)
        self.data["commodityId"] = self.data["commodityId"].astype(int)
        self.data["varietyId"] = self.data["varietyId"].astype(int)
        self.data.loc[self.data['harvestWeek'] == "", 'harvestWeek'] = 0
        self.data["harvestWeek"] = self.data["harvestWeek"].astype(int)
        self.data["rightId"] = self.data["rightId"].astype(str).apply(lambda x: re.sub("[^0-9]+", "", x))
        self.data["mbepValue"] = self.data["mbepValue"].astype(float)
        self.data["quantity"] = self.data["quantity"].astype(float)
        self.relaxations = pd.json_normalize(input_json["relaxation"])
        for values in self.relaxations.values.tolist()[0]:
            if values not in ["Medium", "Loose", "Strict"]:
                # TODO: Change the error code here (DONE)
                self.errors.append({
                    "success": "false",
                    "error": "RGTERR-25",
                    "message": "Relaxation strength is not valid"
                })
        # Relaxation for Quality
        if self.relaxations["quality"].iloc[0] == "Medium":
            self.data["quality"] = self.data["quality"].map(
                {"Band-I": 1, "Band-II": 1, "Band-III": 2, "Band-IV": 2, "Band-V": 3})
        elif self.relaxations["quality"].iloc[0] == "Loose":
            self.data["quality"] = self.data["quality"].map(
                {"Band-I": 1, "Band-II": 1, "Band-III": 1, "Band-IV": 2, "Band-V": 2})
        else:
            self.data["quality"] = self.data["quality"].map(
                {"Band-I": 1, "Band-II": 2, "Band-III": 3, "Band-IV": 4, "Band-V": 5})

        # Relaxation for Farmer Rating
        self.data["rating_class"] = 0
        if self.relaxations["farmerRating"].iloc[0] == "Loose":
            self.data["rating_class"] = 1
        elif self.relaxations["farmerRating"].iloc[0] == "Medium":
            self.data.loc[(self.data['farmerRating'] >= 1) & (self.data['farmerRating'] <= 6), "rating_class"] = 1
            self.data.loc[(self.data['farmerRating'] > 6) & (self.data['farmerRating'] <= 12), "rating_class"] = 2
        else:
            self.data.loc[(self.data['farmerRating'] >= 1) & (self.data['farmerRating'] <= 2.5), "rating_class"] = 1
            self.data.loc[(self.data['farmerRating'] > 2.5) & (self.data['farmerRating'] <= 4), "rating_class"] = 2
            self.data.loc[(self.data['farmerRating'] > 4) & (self.data['farmerRating'] <= 6), "rating_class"] = 3
            self.data.loc[(self.data['farmerRating'] > 6) & (self.data['farmerRating'] <= 8), "rating_class"] = 4
            self.data.loc[(self.data['farmerRating'] > 8) & (self.data['farmerRating'] <= 12), "rating_class"] = 5

        # All Rights must be of same Region
        if len(self.data["regionId"].unique()) > 1:
            self.errors.append({
                "success": "false",
                "error": "RGTERR-004",
                "message": "All Rights must be of same Region"
            })
        elif len(self.data["commodityId"].unique()) > 1:
            self.errors.append({
                "success": "false",
                "error": "RGTERR-005",
                "message": "All Rights must be of same Commodity"
            })
        elif len(self.data["cropType"].unique()) > 1:
            self.errors.append({
                "success": "false",
                "error": "RGTERR-020",
                "message": "All Rights must be of same Crop Type"
            })
        elif self.data['rightId'].duplicated().any():
            self.errors.append({
                "success": "false",
                "error": "RGTERR-024",
                "message": "Every Right ID must be unique"
            })
        self.unique_specs_df = self.data.groupby(
            ['regionId', 'commodityId', "varietyId", "quality",
             "rating_class", "cropType"]).size().reset_index().rename(
            columns={0: 'count'})
        self.unique_specs = self.unique_specs_df.values.tolist()
        self.region_commodity_variety_list = pd.read_sql("""select varietyId , commodityId , gr.regionId
                from cdt_master_data.zonal_variety zv
                inner join cdt_master_data.geo_acz ac on ac.ID = zv.AczID
                inner join cdt_master_data.geo_region gr on gr.StateCode = ac.StateCode and gr.Status = "Active"
                where ZonalCommodityID in (
                select distinct zc.ID 
                from cdt_master_data.regional_commodity rc 
                inner join cdt_master_data.zonal_commodity as zc on rc.ZonalCommodityID = zc.ID and zc.Status = "Active"
                where RegionID in (select RegionID from cdt_master_data.geo_region where Status = "Active") and rc.Status = "Active") and zv.Status = "Active"
                group by VarietyID, CommodityID, gr.RegionID""", self.dbConn)
        self.region_list = list(self.region_commodity_variety_list["regionId"].unique())
        self.commodity_list = list(self.region_commodity_variety_list["commodityId"].unique())
        self.variety_list = list(self.region_commodity_variety_list["varietyId"].unique())
        self.commodity_variety_list = self.region_commodity_variety_list.groupby(
            ['commodityId', "varietyId"]).size().reset_index().rename(
            columns={0: 'count'}).drop(columns=["count"]).values.tolist()
        self.crop_type_list = \
            pd.read_sql("""SELECT Name FROM cdt_master_data.agri_crop_type where Status = "Active";""", self.dbConn)[
                "Name"].to_list()

        self.unique_specs_rchw = self.data.groupby(
            ['regionId', 'commodityId', 'harvestWeek']).size().reset_index().rename(
            columns={0: 'count'})
        # if len(self.region_commodity_variety_list.values.tolist()) > 1:
        condition = []
        for combi in self.unique_specs_rchw.itertuples():
            if combi.harvestWeek == 0:
                condition.append(
                    f"( RegionID = {combi.regionId} and CommodityId = {combi.commodityId})")
            else:
                condition.append(
                    f"( RegionID = {combi.regionId} and CommodityId = {combi.commodityId} and {combi.harvestWeek} between HarvestWeekStart AND if(HarvestWeekEnd  <HarvestWeekStart,HarvestWeekEnd+52,HarvestWeekEnd))")
        self.condition_str = " or ".join(condition)
        query = f"""select rc.RegionID, zc.CommodityID, any_value(MinLotSize) as MinLotSize, any_value(MaxRigtsInLot) as MaxRigtsInLot, ANY_VALUE(HarvestRelaxation) AS HarvestRelaxation
                    from cdt_master_data.regional_commodity rc 
                    inner join cdt_master_data.zonal_commodity zc on rc.ZonalCommodityID = zc.ID
                    where {self.condition_str} group by rc.RegionID, zc.CommodityID;"""
        # print(query)
        self.reg_com = pd.read_sql(query, self.dbConn)
        if len(self.reg_com) == 0:
            self.errors.append({
                # "path": "http://cropdata.tk:8080/lot-aggregation/",
                "success": "false",
                "error": "RGTERR-016",
                "message": "Invalid combination of Region, Commodity and Harvest week provided."

            })

    def lot_aggregation_algo(self):
        if len(self.errors) == 0:
            self.data["error"] = self.data.apply(lambda row: self.check_validation(row), axis=1)
            # self.errors.extend(self.data[self.data.error == False].error.to_list())
            self.data = self.data[self.data.error == True]
            lot_list = []
            lotList = []
            remaining_list = []
            for unq_spc in self.unique_specs:
                min_lot_size = \
                    self.reg_com[(self.reg_com.CommodityID == unq_spc[1]) & (
                            self.reg_com.RegionID == unq_spc[0])].MinLotSize.iloc[
                        0]
                # print(min_lot_size)
                max_rights = \
                    self.reg_com[(self.reg_com.CommodityID == unq_spc[1]) & (
                            self.reg_com.RegionID == unq_spc[0])].MaxRigtsInLot.iloc[
                        0]
                harvest_week_relaxation = self.reg_com[(self.reg_com.CommodityID == unq_spc[1]) & (
                        self.reg_com.RegionID == unq_spc[0])].HarvestRelaxation.iloc[
                    0]
                # print(max_rights)
                lot_data = []
                current_spec_data = self.data[
                    (self.data.regionId == unq_spc[0]) & (self.data.commodityId == unq_spc[1]) & (
                            self.data.varietyId == unq_spc[2]) & (
                            self.data.quality == unq_spc[3]) & (self.data.rating_class == unq_spc[4])]
                quantity_list = current_spec_data.sort_values(by=["quantity"],
                                                              ascending=False)
                right_df = current_spec_data[(current_spec_data["quantity"] >= min_lot_size)]
                for data_1 in right_df.itertuples():
                    if data_1.rightId in lot_list:
                        continue
                    lot_data.append([data_1.rightId])
                    lot_list.append(data_1.rightId)
                quantity_list = quantity_list[~quantity_list["rightId"].isin(lot_list)]
                for items in quantity_list.itertuples():
                    if items.rightId in lot_list:
                        continue

                    # mbep relaxation
                    if self.relaxations["mbepValue"].iloc[0] == "Loose":
                        min_range_mbep = items.mbepValue - (items.mbepValue * 0.2)
                        max_range_mbep = items.mbepValue + (items.mbepValue * 0.2)
                    elif self.relaxations["mbepValue"].iloc[0] == "Medium":
                        min_range_mbep = items.mbepValue - (items.mbepValue * 0.1)
                        max_range_mbep = items.mbepValue + (items.mbepValue * 0.1)
                    else:
                        min_range_mbep = items.mbepValue - (items.mbepValue * 0.05)
                        max_range_mbep = items.mbepValue + (items.mbepValue * 0.05)
                    # harvest week relaxation
                    if self.relaxations["harvestWeek"].iloc[0] == "Loose":
                        min_range_hw = items.harvestWeek - (harvest_week_relaxation + 1)
                        max_range_hw = items.harvestWeek + (harvest_week_relaxation + 1)
                    elif self.relaxations["harvestWeek"].iloc[0] == "Medium":
                        min_range_hw = items.harvestWeek - harvest_week_relaxation
                        max_range_hw = items.harvestWeek + harvest_week_relaxation
                    else:
                        min_range_hw = items.harvestWeek
                        max_range_hw = items.harvestWeek
                    # quantity_list_1 = quantity_list[
                    #     (items.mbepValue - (items.mbepValue * 0.05) <= quantity_list.mbepValue) & (
                    #             quantity_list.mbepValue <= items.mbepValue + (items.mbepValue * 0.05)) & ()]
                    quantity_list_1 = quantity_list[
                        (min_range_mbep <= quantity_list.mbepValue) & (
                                quantity_list.mbepValue <= max_range_mbep) & (
                                min_range_hw <= quantity_list.harvestWeek) & (
                                quantity_list.harvestWeek <= max_range_hw)]
                    for values in quantity_list_1.itertuples():
                        list_quantity = []
                        list_right = []
                        if values.rightId in lot_list:
                            continue
                        while sum(list_quantity) < min_lot_size and len(list_quantity) <= max_rights:
                            if len(list_quantity) > 1 and sum(list_quantity) != min_lot_size and len(
                                    quantity_list_1) == 0:
                                remaining_list.extend(list_right)
                                list_right = []
                                list_quantity = []
                                break
                            if len(list_quantity) == max_rights and sum(list_quantity) < min_lot_size:
                                remaining_list.extend(list_right)
                                break
                            if len(quantity_list_1) == 1 and len(list_quantity) == 0:
                                if values.rightId not in remaining_list:
                                    remaining_list.append(values.rightId)
                                    lot_list.append(values.rightId)
                                    break
                            if values.rightId not in list_right:
                                list_quantity.append(values.quantity)
                                list_right.append(values.rightId)
                                lot_list.append(values.rightId)
                                quantity_list_1 = quantity_list_1[~quantity_list_1.rightId.isin(list_right)]
                            nest = min_lot_size - sum(list_quantity)
                            min_value_df = quantity_list_1[quantity_list_1.quantity >= nest].sort_values(
                                by=["quantity"],
                                ascending=True)
                            if len(min_value_df) > 0:
                                min_value = min_value_df["quantity"].iloc[0]
                                right_id_min = min_value_df["rightId"].iloc[0]
                                list_quantity.append(min_value)
                                list_right.append(right_id_min)
                                lot_list.append(right_id_min)
                                quantity_list_1 = quantity_list_1[~quantity_list_1.rightId.isin(list_right)]
                            else:
                                if len(list_quantity) == 0:
                                    break
                                max_value_df = quantity_list_1.sort_values(by=["quantity"], ascending=False)
                                if len(max_value_df) > 0:
                                    max_value = max_value_df["quantity"].iloc[0]
                                    right_id_min = max_value_df["rightId"].iloc[0]
                                    list_quantity.append(max_value)
                                    list_right.append(right_id_min)
                                    lot_list.append(right_id_min)
                                    quantity_list_1 = quantity_list_1[~quantity_list_1.rightId.isin(list_right)]
                                else:
                                    remaining_list.extend(list_right)
                                    break
                            if len(list_right) > 0 and sum(list_quantity) >= min_lot_size:
                                lot_data.append(list_right)
                lotList.extend(lot_data)
        else:
            lotList = []
            remaining_list = self.data.rightId.to_list()

        result = {
            "lotList": lotList,
            "remainingRights": remaining_list,
            "errors": self.errors
        }
        return result

    def check_validation(self, data):
        flag = True
        if data["commodityId"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "Commodity ID must be greater than zero",
                "error": "RGTERR-021"
            })
            flag = False
        if data["commodityId"] not in self.commodity_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid CommodityId provided",
                "error": "RGTERR-014"
            })
            flag = False

        if data["varietyId"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "Variety ID must be greater than zero",
                "error": "RGTERR-010"
            })
            flag = False
        if data["varietyId"] not in self.variety_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid VarietyId provided",
                "error": "RGTERR-015"
            })
            flag = False

        if [data["commodityId"], data["varietyId"]] not in self.commodity_variety_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid combination of Commodity and Variety provided",
                "error": "RGTERR-019"
            })
            flag = False

        if data["regionId"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "Region ID must be greater than zero",
                "error": "RGTERR-009"
            })
            flag = False
        if data["regionId"] not in self.region_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid RegionId provided",
                "error": "RGTERR-013"
            })
            flag = False

        if data["quality"] not in range(1, 6, 1):
            self.errors.append({
                "id": data["rightId"],
                "message": "Quality must be in between Band-I to Band-V",
                "error": "RGTERR-006"
            })
            flag = False
        if data["cropType"] not in ["Harvested", "Warehoused"]:
            if data["harvestWeek"] not in range(1, 53, 1):
                self.errors.append({
                    "id": data["rightId"],
                    "message": "Harvest Week must be in between 1 to 52",
                    "error": "RGTERR-008"
                })
                flag = False
            query = f"""select count(*)
                                from cdt_master_data.regional_commodity rc 
                                inner join cdt_master_data.zonal_commodity zc on rc.ZonalCommodityID = zc.ID
                                where RegionID = {data.regionId} and CommodityId = {data.commodityId} and
                                 {data.harvestWeek} between HarvestWeekStart AND 
                                 if(HarvestWeekEnd  <HarvestWeekStart,HarvestWeekEnd+52,HarvestWeekEnd);"""
            exc = self.dbCursor.execute(query)
            harvest_week_verify = self.dbCursor.fetchone()
            if harvest_week_verify[0] == 0:
                self.errors.append({
                    "id": data["rightId"],
                    "message": "Incorrect Harvest Week for the given combination of Region and Commodity",
                    "error": "RGTERR-017"
                })
                flag = False

        if data["mbepValue"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "MBEP Value must be greater than zero",
                "error": "RGTERR-011"
            })
            flag = False

        if data["farmerRating"] not in range(1, 13, 1):
            self.errors.append({
                "id": data["rightId"],
                "message": "Farmer Rating must be in between 1 to 12",
                "error": "RGTERR-007"
            })
            flag = False

        if data["quantity"] <= 0:
            self.errors.append({
                "id": data["rightId"],
                "message": "Quantity Value must be greater than zero",
                "error": "RGTERR-012"
            })
            flag = False

        if data["cropType"] not in self.crop_type_list:
            self.errors.append({
                "id": data["rightId"],
                "message": "Invalid CropType provided",
                "error": "RGTERR-018"
            })
            flag = False

        # if type(data["rightId"]) is str:
        #     self.errors.append({
        #         "id": data["rightId"],
        #         "message": "Right ID should not be string",
        #         "error": "RGTERR-027"
        #     })
        #     flag = False
        if type(data["rightId"]) is float:
            self.errors.append({
                "id": data["rightId"],
                "message": "Right ID should not be Float",
                "error": "RGTERR-029"
            })
        # else:
        #     if data["rightId"] <= 1:
        #         self.errors.append({
        #             "id": data["rightId"],
        #             "message": "Right ID must be greater than zero",
        #             "error": "RGTERR-022"
        #         })
        #         flag = False
        if len(str(data["rightId"])) != 19:
            self.errors.append({
                "id": data["rightId"],
                "message": "Right ID must be of 19 digits",
                "error": "RGTERR-023"
            })
            flag = False

        if not flag:
            return False
        else:
            return True


class Spec_source(APIView):
    @authentication_classes(APIKeyValidator)
    def get(self, request):
        if "apiKey" in request.query_params:
            obj_lot = spec_source()
            result = obj_lot.create_spec_source()
            return JsonResponse(result, status=200, safe=False)
        else:
            return JsonResponse({
                "success": "false",
                "error": "RGTERR-001",
                "message": "Request should contain apiKey"
            }, status=417)


class spec_source:
    def __init__(self):
        self.dbConn = mysql.connector.connect(
            host=os.environ["DBHOST"],
            port=os.environ["DBPORT"],
            user=os.environ["DBUSER"],
            password=os.environ["DBPASS"],
            auth_plugin='mysql_native_password',
        )
        self.dbCursor = self.dbConn.cursor()

    def create_spec_source(self):
        query = f"""select varietyId , commodityId , gr.regionId
                from cdt_master_data.zonal_variety zv
                inner join cdt_master_data.geo_acz ac on ac.ID = zv.AczID
                inner join cdt_master_data.geo_region gr on gr.StateCode = ac.StateCode and gr.Status = "Active"
                where ZonalCommodityID in (
                select distinct zc.ID 
                from cdt_master_data.regional_commodity rc 
                inner join cdt_master_data.zonal_commodity as zc on rc.ZonalCommodityID = zc.ID and zc.Status = "Active"
                where RegionID in (select RegionID from cdt_master_data.geo_region where Status = "Active") and rc.Status = "Active") and zv.Status = "Active"
                group by VarietyID, CommodityID, gr.RegionID;"""
        dataframe = pd.read_sql(query, self.dbConn)
        json = dataframe.to_dict("records")
        return json


class TestSpec(APIView):
    @authentication_classes(APIKeyValidator)
    def get(self, request):
        if "regionId" in request.query_params and "commodityId" in request.query_params and "varietyId" in request.query_params and "cropType" in request.query_params and "page" in request.query_params:
            regionId = request.query_params["regionId"]
            commodityId = request.query_params["commodityId"]
            varietyId = request.query_params["varietyId"]
            cropType = request.query_params["cropType"]
            page = request.query_params["page"]
            obj_lot = test_spec(regionId, commodityId, varietyId, cropType, page)
            result = obj_lot.create_test_spec()
            return JsonResponse(result, status=200, safe=False)
        else:
            return JsonResponse({
                "success": "false",
                "error": "RGTERR-001",
                "message": "Request should contain apiKey, regionId, commodityId, varietyId, cropType and page"
            }, status=417)


class test_spec:
    def __init__(self, regionId, commodityId, varietyId, cropType, page):
        self.dbConn = mysql.connector.connect(
            host=os.environ["DBHOST"],
            port=os.environ["DBPORT"],
            user=os.environ["DBUSER"],
            password=os.environ["DBPASS"],
            auth_plugin='mysql_native_password',
        )
        self.dbCursor = self.dbConn.cursor()
        self.regionId = regionId
        self.commodityId = commodityId
        self.varietyId = varietyId
        self.cropType = cropType
        self.page = page

    def create_test_spec(self):
        limitl = 0 if int(self.page) == 1 else (int(self.page) - 1) * 5000 + 1
        limitu = 5000
        query = f"""SELECT
                    pmp.RegionID AS regionId,
                    pmp.CommodityID AS commodityId,
                    pmp.VarietyID AS varietyId,
                    pmp.mbep AS mbepValue,
                    pmp.cropType,

                    FLOOR(RAND() * (12 - 1) + 1) AS farmerRating,
                    ROUND(RAND() * (if(t.HarvestWeekEnd < t.HarvestWeekStart, t.HarvestWeekStart, t.HarvestWeekEnd) - t.HarvestWeekStart) + t.HarvestWeekStart) AS harvestWeek,
                    (IF(pmp.VarietyID % 5 = 0 AND t.HarvestWeekStart % 7 = 0, FLOOR(RAND() * (30 - 1) + 1), FLOOR(RAND() * (15 - 1) + 1))) AS quantity,
                    CONCAT('120', '000000', FLOOR(RAND(10) * (9999999999 - 1000000000) + 1000000000 + UNIX_TIMESTAMP())) AS rightId,

                    ab.Name AS quality,
                    gv.VillageCode AS villageCode,

                    '' AS 'flexi_1',
                    '' AS 'flexi_2',
                    '' AS 'flexi_3',
                    '' AS 'flexi_4',
                    '' AS 'flexi_5'
                FROM (
                    select 
                        zv.CommodityID,
                        zv.VarietyID,
                        zv.HarvestWeekStart,
                        zv.HarvestWeekEnd,
                        rc.RegionID,
                        zv.AczID
                    from cdt_master_data.zonal_variety as zv
                    inner join cdt_master_data.regional_commodity as rc on rc.ZonalCommodityID = zv.ZonalCommodityID
                ) as t
                INNER JOIN cdt_master_data.pricing_master_mbep as pmp ON (
                    pmp.CommodityID = t.CommodityID
                    and pmp.VarietyID = t.VarietyID
                    and pmp.RegionID = t.RegionID
                )
                inner join cdt_master_data.geo_village as gv on gv.AczID = t.AczID and gv.status = 'Active'
                INNER JOIN cdt_master_data.agri_band as ab ON pmp.BandID = ab.ID
                WHERE 
                    t.RegionID = {self.regionId}
                    AND pmp.CropType = "{self.cropType}"
                    AND t.commodityId = {self.commodityId}
                    AND t.varietyId = {self.varietyId}
                LIMIT {limitl}, {limitu};"""
        # print(query)
        dataframe = pd.read_sql(query, self.dbConn)
        json = dataframe.to_dict("records")
        return json
