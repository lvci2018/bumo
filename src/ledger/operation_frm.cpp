/*
	bumo is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	bumo is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with bumo.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <utils/strings.h>
#include <utils/base_int.h>
#include <ledger/ledger_manager.h>
#include "transaction_frm.h"
#include "operation_frm.h"
#include "contract_manager.h"
#include "order_frm.h"
#include "order_exchange.h"
#include <common/price.h>

namespace bumo {
	OperationFrm::OperationFrm(const protocol::Operation &operation, TransactionFrm* tran, int32_t index) :
		operation_(operation), transaction_(tran), index_(index), ope_fee_(0){}

	OperationFrm::~OperationFrm() {}

	Result OperationFrm::GetResult() const {
		return result_;
	}

	int64_t OperationFrm::GetOpeFee() const {
		return ope_fee_;
	}

	Result OperationFrm::CheckValid(const protocol::Operation& operation, const std::string &source_address) {
		Result result;
		result.set_code(protocol::ERRCODE_SUCCESS);
		auto type = operation.type();
		const protocol::OperationCreateAccount& create_account = operation.create_account();
		const protocol::OperationPayment& payment = operation.payment();
		const protocol::OperationIssueAsset& issue_asset = operation.issue_asset();

		if (!bumo::PublicKey::IsAddressValid(source_address)) {
			result.set_code(protocol::ERRCODE_INVALID_ADDRESS);
			result.set_desc(utils::String::Format("Source address should be a valid account address"));
			return result;
		}
		//const auto &issue_property = issue_asset.
		switch (type) {
		case protocol::Operation_Type_CREATE_ACCOUNT:
		{
			if (!bumo::PublicKey::IsAddressValid(create_account.dest_address())) {
				result.set_code(protocol::ERRCODE_INVALID_ADDRESS);
				result.set_desc(utils::String::Format("Dest account address(%s) invalid", create_account.dest_address().c_str()));
				break;
			}

			if (!create_account.has_priv()) {
				result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
				result.set_desc(utils::String::Format("Dest account address(%s) has no priv object", create_account.dest_address().c_str()));
				break;
			} 

			const protocol::AccountPrivilege &priv = create_account.priv();
			if (priv.master_weight() < 0 || priv.master_weight() > UINT32_MAX) {
				result.set_code(protocol::ERRCODE_WEIGHT_NOT_VALID);
				result.set_desc(utils::String::Format("Master weight(" FMT_I64 ") is larger than %u or less 0", priv.master_weight(), UINT32_MAX));
				break;
			}

			//for signers
			std::set<std::string> duplicate_set;
			bool shouldBreak = false;
			for (int32_t i = 0; i < priv.signers_size(); i++) {
				const protocol::Signer &signer = priv.signers(i);
				if (signer.weight() < 0 || signer.weight() > UINT32_MAX) {
					result.set_code(protocol::ERRCODE_WEIGHT_NOT_VALID);
					result.set_desc(utils::String::Format("Signer weight(" FMT_I64 ") is larger than %u or less 0", signer.weight(), UINT32_MAX));
					shouldBreak = true;
					break;
				}

				if (signer.address() == create_account.dest_address()) {
					result.set_code(protocol::ERRCODE_INVALID_ADDRESS);
					result.set_desc(utils::String::Format("Signer address(%s) can't be equal to the source address", signer.address().c_str()));
					shouldBreak = true;
					break;
				}

				if (!PublicKey::IsAddressValid(signer.address())) {
					result.set_code(protocol::ERRCODE_INVALID_ADDRESS);
					result.set_desc(utils::String::Format("Signer address(%s) is not valid", signer.address().c_str()));
					shouldBreak = true;
					break;
				}

				if (duplicate_set.find(signer.address()) != duplicate_set.end()) {
					result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
					result.set_desc(utils::String::Format("Signer address(%s) duplicated", signer.address().c_str()));
					shouldBreak = true;
					break;
				} 

				duplicate_set.insert(signer.address());
			}
			if (shouldBreak) break;

			//for threshold
			if (!priv.has_thresholds()) {
				result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
				result.set_desc(utils::String::Format("dest account address(%s) has no threshold object", create_account.dest_address().c_str()));
				break;
			}

			const protocol::AccountThreshold &threshold = priv.thresholds();
			if (threshold.tx_threshold() < 0) {
				result.set_code(protocol::ERRCODE_THRESHOLD_NOT_VALID);
				result.set_desc(utils::String::Format("Low threshold(" FMT_I64 ") is less than 0", threshold.tx_threshold()));
				break;
			}

			std::set<int32_t> duplicate_type;
			for (int32_t i = 0; i < threshold.type_thresholds_size(); i++) {
				const protocol::OperationTypeThreshold  &type_thresholds = threshold.type_thresholds(i);
				if (type_thresholds.type() > 100 || type_thresholds.type() <= 0) {
					result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
					result.set_desc(utils::String::Format("Operation type(%u) not support", type_thresholds.type()));
					break;
				}

				if (type_thresholds.threshold() < 0) {
					result.set_code(protocol::ERRCODE_THRESHOLD_NOT_VALID);
					result.set_desc(utils::String::Format("Operation type(%d) threshold(" FMT_I64 ") is less than 0", (int32_t)type_thresholds.type(), type_thresholds.threshold()));
					break;
				}

				if (duplicate_type.find(type_thresholds.type()) != duplicate_type.end()) {
					result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
					result.set_desc(utils::String::Format("Operation type(%u) duplicated", type_thresholds.type()));
					break;
				}

				duplicate_type.insert(type_thresholds.type());
			}

			//if it's contract then {master_weight:0 , thresholds:{tx_threshold:1} }
			if (create_account.contract().payload() != ""){
				if (create_account.contract().payload().size() > General::CONTRACT_CODE_LIMIT) {
					result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
					result.set_desc(utils::String::Format("Contract payload size(" FMT_SIZE ") > limit(%d)",
						create_account.contract().payload().size(), General::CONTRACT_CODE_LIMIT));
					break;
				}

				if (!(priv.master_weight() == 0 &&
					priv.signers_size() == 0 &&
					threshold.tx_threshold() == 1 &&
					threshold.type_thresholds_size() == 0
					)) {
					result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
					result.set_desc(utils::String::Format("Contract account 'priv' config must be({master_weight:0, thresholds:{tx_threshold:1}})"));
					break;
				}

				std::string src = create_account.contract().payload();
				result = ContractManager::Instance().SourceCodeCheck(Contract::TYPE_V8, src);
			}

			for (int32_t i = 0; i < create_account.metadatas_size(); i++){
				const auto kp = create_account.metadatas(i);
				if (kp.key().size() == 0 || kp.key().size() > General::METADATA_KEY_MAXSIZE) {
					result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
					result.set_desc(utils::String::Format("Length of the key should be between [1, %d]. key=%s,key.length=%d",
						General::METADATA_KEY_MAXSIZE, kp.key().c_str(), kp.key().length()));
				}

				if (kp.value().size() > General::METADATA_MAX_VALUE_SIZE){
					result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
					result.set_desc(utils::String::Format("Length of the value should be between [1, %d].key=%s,value.length=%d",
						General::METADATA_MAX_VALUE_SIZE, kp.key().c_str(), kp.value().length()));
				}
			}
			break;
		}
		case protocol::Operation_Type_PAYMENT:
		{
			if (payment.has_asset()){

				if ((protocol::AssetKey_Type)payment.asset().key().type() != protocol::AssetKey_Type_UNLIMIT && (protocol::AssetKey_Type)payment.asset().key().type() != protocol::AssetKey_Type_LIMIT){
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("Payment asset type must be limit or unlimit"));
					break;
				}

				if (payment.asset().amount() <= 0) {
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("payment amount should be bigger than 0"));
					break;
				}

				std::string trim_code = payment.asset().key().code();
				//utils::String::Trim(trim_code);
				if (trim_code.size() == 0 || trim_code.size() > General::ASSET_CODE_MAX_SIZE) {
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("asset code length should between (0,64]"));
					break;
				}

				if (!bumo::PublicKey::IsAddressValid(payment.asset().key().issuer())) {
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("asset issuer should be a valid account address"));
					break;
				}

				if ((protocol::AssetKey_Type)payment.asset().key().type() == protocol::AssetKey_Type_SELF_COIN){
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("Payment asset(%s:%s:%d) can not pay self coin", payment.asset().key().issuer().c_str(), payment.asset().key().code().c_str(), (int)payment.asset().key().type()));
					break;
				}
			}

			if (source_address == payment.dest_address()) {
				result.set_code(protocol::ERRCODE_ACCOUNT_SOURCEDEST_EQUAL);
				result.set_desc(utils::String::Format("Source address(%s) equal to dest address", source_address.c_str()));
				break;
			}

			if (!bumo::PublicKey::IsAddressValid(payment.dest_address())) {
				result.set_code(protocol::ERRCODE_INVALID_ADDRESS);
				result.set_desc(utils::String::Format("Dest address should be a valid account address"));
				break;
			}
			break;
		}

		case protocol::Operation_Type_ISSUE_ASSET:
		{
			if (issue_asset.amount() <= 0) {
				result.set_code(protocol::ERRCODE_ASSET_INVALID);
				result.set_desc(utils::String::Format("Issue unlimit asset amount should be bigger than 0"));
				break;
			}
			
			std::string trim_code = issue_asset.code();
			trim_code = utils::String::Trim(trim_code);
			if (trim_code.size() == 0 || trim_code.size() > General::ASSET_CODE_MAX_SIZE ||
				trim_code.size() != issue_asset.code().size()) {
				result.set_code(protocol::ERRCODE_ASSET_INVALID);
				result.set_desc(utils::String::Format("Asset code length should between (0,64]"));
				break;
			}

			break;
		}
		case protocol::Operation_Type_SET_METADATA:
		{
			const protocol::OperationSetMetadata &set_metadata = operation.set_metadata();

			std::string trim = set_metadata.key();
			if (trim.size() == 0 || trim.size() > General::METADATA_KEY_MAXSIZE) {
				result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
				result.set_desc(utils::String::Format("Length of the key should be between [1, %d]. key=%s,key.length=%d",
					General::METADATA_KEY_MAXSIZE, trim.c_str(), trim.length()));
				break;
			}

			if (set_metadata.value().size() > General::METADATA_MAX_VALUE_SIZE) {
				result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
				result.set_desc(utils::String::Format("Length of the value should be between [0, %d]. key=%s,value.length=%d",
					General::METADATA_MAX_VALUE_SIZE, trim.c_str(), set_metadata.value().length()));
				break;
			}


			break;
		}
		case protocol::Operation_Type_SET_SIGNER_WEIGHT:
		{
			const protocol::OperationSetSignerWeight &operation_setoptions = operation.set_signer_weight();
			if (operation_setoptions.master_weight() < -1 || operation_setoptions.master_weight() > UINT32_MAX) {
				result.set_code(protocol::ERRCODE_WEIGHT_NOT_VALID);
				result.set_desc(utils::String::Format("Master weight(" FMT_I64 ") is larger than %u or less -1", operation_setoptions.master_weight(), UINT32_MAX));
				break;
			}

			for (int32_t i = 0; i < operation_setoptions.signers_size(); i++) {
				const protocol::Signer &signer = operation_setoptions.signers(i);
				if (signer.weight() < 0 || signer.weight() > UINT32_MAX) {
					result.set_code(protocol::ERRCODE_WEIGHT_NOT_VALID);
					result.set_desc(utils::String::Format("Signer weight(" FMT_I64 ") is larger than %u or less 0", signer.weight(), UINT32_MAX));
					break;
				}

				if (signer.address() == source_address) {
					result.set_code(protocol::ERRCODE_INVALID_ADDRESS);
					result.set_desc(utils::String::Format("Signer address(%s) can't be equal to the source address", signer.address().c_str()));
					break;
				}

				if (!PublicKey::IsAddressValid(signer.address())) {
					result.set_code(protocol::ERRCODE_INVALID_ADDRESS);
					result.set_desc(utils::String::Format("Signer address(%s) is not valid", signer.address().c_str()));
					break;
				}
			}

			break;
		}
		case protocol::Operation_Type_SET_THRESHOLD:
		{
			const protocol::OperationSetThreshold operation_setoptions = operation.set_threshold();

			if ( operation_setoptions.tx_threshold() < -1) {
				result.set_code(protocol::ERRCODE_THRESHOLD_NOT_VALID);
				result.set_desc(utils::String::Format("Low threshold(" FMT_I64 ") is less than -1", operation_setoptions.tx_threshold()));
				break;
			}

			for (int32_t i = 0; i < operation_setoptions.type_thresholds_size(); i++) {
				const protocol::OperationTypeThreshold  &type_thresholds = operation_setoptions.type_thresholds(i);
				if (type_thresholds.type() > 100 || type_thresholds.type() <= 0) {
					result.set_code(protocol::ERRCODE_THRESHOLD_NOT_VALID);
					result.set_desc(utils::String::Format("Operation type(%u) not support", type_thresholds.type()));
					break;
				}

				if (type_thresholds.threshold()  < 0 ) {
					result.set_code(protocol::ERRCODE_THRESHOLD_NOT_VALID);
					result.set_desc(utils::String::Format("Operation type(%d) threshold(" FMT_I64 ") is less than 0", (int32_t)type_thresholds.type(), type_thresholds.threshold()));
					break;
				}
			}
			break;
		}
		case protocol::Operation_Type_PAY_COIN:
		{
			const protocol::OperationPayCoin &pay_coin = operation.pay_coin();
			if (pay_coin.amount() < 0){
				result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
				result.set_desc(utils::String::Format("Amount should be bigger than 0"));
			}

			if (source_address == pay_coin.dest_address()) {
				result.set_code(protocol::ERRCODE_ACCOUNT_SOURCEDEST_EQUAL);
				result.set_desc(utils::String::Format("Source address(%s) equal to dest address", source_address.c_str()));
				break;
			}

			if (!bumo::PublicKey::IsAddressValid(pay_coin.dest_address())) {
				result.set_code(protocol::ERRCODE_INVALID_ADDRESS);
				result.set_desc(utils::String::Format("Dest address should be a valid account address"));
				break;
			}
			break;
		}
		/*case protocol::Operation_Type_LOG:
		{
			const protocol::OperationLog &log = operation.log();
			if (log.topic().size() == 0 || log.topic().size() > General::TRANSACTION_LOG_TOPIC_MAXSIZE ){
				result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
				result.set_desc(utils::String::Format("Log's parameter topic should be (0,%d]", General::TRANSACTION_LOG_TOPIC_MAXSIZE));
				break;
			}
			for (int i = 0; i < log.datas_size();i++) {
				if (log.datas(i).size() == 0 || log.datas(i).size() > General::TRANSACTION_LOG_DATA_MAXSIZE){
					result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
					result.set_desc(utils::String::Format("Log's parameter data should be (0, %d]",General::TRANSACTION_LOG_DATA_MAXSIZE));
					break;
				}
			}
			break;
		}*/
		case protocol::Operation_Type_PROCESS_ORDER:
		{
			const protocol::OperationProcessOrder operation_process_order = operation.process_order();

			//cacel order
			if (!operation_process_order.order_id().empty()){
				if (operation_process_order.amount() != 0){
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("order id(%s) excute cancle ,amount must be zero", operation_process_order.order_id().c_str()));
					break;
				}
			}

			//insert order
			if (operation_process_order.order_id().empty()){

				if (operation_process_order.amount() == 0){
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("insert order amount must be not zero"));
					break;
				}


				if (!operation_process_order.has_selling() || !operation_process_order.has_buying()){
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("order must has selling and buying "));
					break;
				}

				std::string trim_code = operation_process_order.selling().code();
				if (trim_code.size() == 0 || trim_code.size() > General::ASSET_CODE_MAX_SIZE) {
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("asset code length should between (0,64]"));
					break;
				}

				if (!bumo::PublicKey::IsAddressValid(operation_process_order.selling().issuer())) {
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("asset issuer should be a valid account address"));
					break;
				}

				trim_code = operation_process_order.buying().code();
				if (trim_code.size() == 0 || trim_code.size() > General::ASSET_CODE_MAX_SIZE) {
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("asset code length should between (0,64]"));
					break;
				}

				if (!bumo::PublicKey::IsAddressValid(operation_process_order.buying().issuer())) {
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("asset issuer should be a valid account address"));
					break;
				}

				AccountFrm::pointer seller;
				if (!Environment::AccountFromDB(operation_process_order.selling().issuer(), seller)) {
					result.set_code(protocol::ERRCODE_ACCOUNT_NOT_EXIST);
					result.set_desc(utils::String::Format("Source account(%s) not exist", operation_process_order.selling().issuer().c_str()));
					LOG_ERROR("%s", result.desc().c_str());
					break;
				}

				protocol::AssetStore asset_s;
				if (!seller->GetAsset(operation_process_order.selling(), asset_s)){
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("asset(%s:%s:%d) not exist", asset_s.key().issuer().c_str(), asset_s.key().code().c_str(), asset_s.key().type()));
					break;
				}

				AccountFrm::pointer buyer;
				if (!Environment::AccountFromDB(operation_process_order.buying().issuer(), buyer)) {
					result.set_code(protocol::ERRCODE_ACCOUNT_NOT_EXIST);
					result.set_desc(utils::String::Format("Source account(%s) not exist", operation_process_order.buying().issuer().c_str()));
					LOG_ERROR("%s", result.desc().c_str());
					break;
				}

				protocol::AssetStore asset_b;
				if (!seller->GetAsset(operation_process_order.buying(), asset_b)){
					result.set_code(protocol::ERRCODE_ASSET_INVALID);
					result.set_desc(utils::String::Format("asset(%s:%s:%d) not exist", asset_b.key().issuer().c_str(), asset_b.key().code().c_str(), asset_b.key().type()));
					break;
				}
				if (operation_process_order.fee_percent() < asset_b.property().fee_percent()){
					result.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
					result.set_desc(utils::String::Format("asset(%s:%s:%d) fee percent is lower(%d)", asset_b.key().issuer().c_str(), asset_b.key().code().c_str(), asset_b.key().type(), asset_s.property().fee_percent()));
					break;
				}

			}

			break;
		}
		case protocol::Operation_Type_REGISTER_ASSET:
		{
			const protocol::OperationRegisterAsset &register_asset = operation.register_asset();

			if (!register_asset.has_property()){
				result.set_code(protocol::ERRCODE_ASSET_INVALID);
				result.set_desc(utils::String::Format("Register limit asset should has property feild"));
				break;
			}

			if (register_asset.property().max_supply() <= 0 || register_asset.property().issued_amount() != 0) {
				result.set_code(protocol::ERRCODE_ASSET_INVALID);
				result.set_desc(utils::String::Format("Register limit asset max_supply should be bigger than 0, issued_amount must be 0"));
				break;
			}

			std::string trim_code = register_asset.code();
			trim_code = utils::String::Trim(trim_code);
			if (trim_code.size() == 0 || trim_code.size() > General::ASSET_CODE_MAX_SIZE ||
				trim_code.size() != register_asset.code().size()) {
				result.set_code(protocol::ERRCODE_ASSET_INVALID);
				result.set_desc(utils::String::Format("Asset code length should between (0,64]"));
				break;
			}

			break;
		}
		case protocol::Operation_Type_SET_ASSET_FEE:
		{			
			const protocol::OperationSetAssetFee &asset_fee = operation.set_asset_fee();

			if (asset_fee.fee() < 0) {
				result.set_code(protocol::ERRCODE_ASSET_INVALID);
				result.set_desc(utils::String::Format("Set asset fee should be bigger than  or equel 0"));
				break;
			}

			std::string trim_code = asset_fee.code();
			if (trim_code.size() == 0 || trim_code.size() > General::ASSET_CODE_MAX_SIZE) {
				result.set_code(protocol::ERRCODE_ASSET_INVALID);
				result.set_desc(utils::String::Format("Set asset fee code length should between (0,64]"));
				break;
			}
			break;
		}

		case protocol::Operation_Type_Operation_Type_INT_MIN_SENTINEL_DO_NOT_USE_:
			break;
		case protocol::Operation_Type_Operation_Type_INT_MAX_SENTINEL_DO_NOT_USE_:
			break;
		default:{
			result.set_code(protocol::ERRCODE_INVALID_PARAMETER);
			result.set_desc(utils::String::Format("Operation type(%d) invalid", type));
			break;
		}
		}

		return result;
	}

	bool OperationFrm::CheckSignature(std::shared_ptr<Environment> txenvironment) {
		std::string source_address_ = operation_.source_address();
		if (source_address_.size() == 0) {
			source_address_ = transaction_->GetSourceAddress();
		}

		if (!txenvironment->GetEntry(source_address_, source_account_)) {
			result_.set_code(protocol::ERRCODE_ACCOUNT_NOT_EXIST);
			result_.set_desc(utils::String::Format("Source account(%s) not exist", source_address_.c_str()));
			return false;
		}

		utils::StringVector vec;
		vec.push_back(source_address_);
		if (!transaction_->SignerHashPriv(source_account_, operation_.type())) {
			LOG_ERROR("Check operation's signature failed");
			result_.set_code(protocol::ERRCODE_INVALID_SIGNATURE);
			result_.set_desc(utils::String::Format("Check operation's signature failed"));
			return false;
		}

		return true;
	}


	Result OperationFrm::Apply(std::shared_ptr<Environment>  environment) {
		std::string source_address = operation_.source_address();
		if (source_address.size() == 0) {
			source_address = transaction_->GetSourceAddress();
		}
		if (!environment->GetEntry(source_address, source_account_)) {
			result_.set_code(protocol::ERRCODE_ACCOUNT_NOT_EXIST);
			result_.set_desc(utils::String::Format("Source address(%s) not exist", source_address.c_str()));
			return result_;
		}
		auto type = operation_.type();
		OptFee(type);
		switch (type) {
		case protocol::Operation_Type_UNKNOWN:
			break;
		case protocol::Operation_Type_CREATE_ACCOUNT:
			CreateAccount(environment);
			break;
		case protocol::Operation_Type_PAYMENT:
			Payment(environment);
			break;
		case protocol::Operation_Type_ISSUE_ASSET:
			IssueAsset(environment);
			break;
		case protocol::Operation_Type_SET_METADATA:
			SetMetaData(environment);
			break;
		case protocol::Operation_Type_SET_SIGNER_WEIGHT:
			SetSignerWeight(environment);
			break;
		case protocol::Operation_Type_SET_THRESHOLD:
			SetThreshold(environment);
			break;
		case protocol::Operation_Type_PAY_COIN:
			PayCoin(environment);
			break;
		/*case protocol::Operation_Type_LOG:
			Log(environment);
			break;*/
		case protocol::Operation_Type_PROCESS_ORDER:
			ProcessOrder(environment);
			break;
		case protocol::Operation_Type_REGISTER_ASSET:
			RegisterAsset(environment);
			break;
		case protocol::Operation_Type_SET_ASSET_FEE:
			SetAssetFee(environment);
			break;
		case protocol::Operation_Type_Operation_Type_INT_MIN_SENTINEL_DO_NOT_USE_:
			break;
		case protocol::Operation_Type_Operation_Type_INT_MAX_SENTINEL_DO_NOT_USE_:
			break;
		default:
			result_.set_code(protocol::ERRCODE_INVALID_PARAMETER);
			result_.set_desc(utils::String::Format("type(%d) not support", type));
			break;
		}
		return result_;
	}

	void OperationFrm::CreateAccount(std::shared_ptr<Environment> environment) {
		//auto &environment = LedgerManager::Instance().execute_environment_;
		const protocol::OperationCreateAccount& createaccount = operation_.create_account();
		do {
			std::shared_ptr<AccountFrm> dest_account;

			if (environment->GetEntry(createaccount.dest_address(), dest_account)) {
				result_.set_code(protocol::ERRCODE_ACCOUNT_DEST_EXIST);
				result_.set_desc(utils::String::Format("Dest address(%s) already exist", createaccount.dest_address().c_str()));
				break;
			}

			int64_t base_reserve = LedgerManager::Instance().GetCurFeeConfig().base_reserve();
			if (createaccount.init_balance() < base_reserve) {
				result_.set_code(protocol::ERRCODE_ACCOUNT_INIT_LOW_RESERVE);
				std::string error_desc = utils::String::Format("Dest address init balance (" FMT_I64 ") not enough for base_reserve (" FMT_I64 ")", createaccount.init_balance(), base_reserve);
				result_.set_desc(error_desc);
				LOG_ERROR("%s", error_desc.c_str());
				break;
			}
			if (source_account_->GetAccountBalance() - base_reserve < createaccount.init_balance()) {
				result_.set_code(protocol::ERRCODE_ACCOUNT_LOW_RESERVE);
				std::string error_desc = utils::String::Format("Source account(%s) balance(" FMT_I64 ") - base_reserve(" FMT_I64 ") not enough for init balance(" FMT_I64 ")", 
				source_account_->GetAccountAddress().c_str(),source_account_->GetAccountBalance(), base_reserve, createaccount.init_balance());
				result_.set_desc(error_desc);
				LOG_ERROR("%s", error_desc.c_str());
				break;
			}
			source_account_->AddBalance(-1 * createaccount.init_balance());

			protocol::Account account;
			account.set_balance(createaccount.init_balance());
			account.mutable_priv()->CopyFrom(createaccount.priv());
			account.set_address(createaccount.dest_address());
			account.mutable_contract()->CopyFrom(createaccount.contract());
			dest_account = std::make_shared<AccountFrm>(account);

			bool success = true;
			for (int i = 0; i < createaccount.metadatas_size(); i++) {
				protocol::KeyPair kp;
				kp.CopyFrom(createaccount.metadatas(i));
				if (kp.version() != 0 && kp.version() != 1){
					success = false;
					break;
				}
				kp.set_version(1);
				dest_account->SetMetaData(kp);
			}
			if (!success){
				result_.set_code(protocol::ERRCODE_INVALID_DATAVERSION);
				result_.set_desc(utils::String::Format(
					"set meatadata while create account(%s) version should be 0 or 1 ",
					dest_account->GetAccountAddress().c_str()));
				
				break;
			}

			environment->AddEntry(dest_account->GetAccountAddress(), dest_account);

			/*std::string javascript = dest_account->GetProtoAccount().contract().payload();
			if (!javascript.empty()) {

				ContractParameter parameter;
				parameter.code_ = javascript;
				parameter.input_ = createaccount.init_input();
				parameter.this_address_ = createaccount.dest_address();
				parameter.sender_ = source_account_->GetAccountAddress();
				parameter.ope_index_ = index_;
				parameter.timestamp_ = transaction_->ledger_->value_->close_time();
				parameter.blocknumber_ = transaction_->ledger_->value_->ledger_seq();
				parameter.ledger_context_ = transaction_->ledger_->lpledger_context_;
				parameter.pay_coin_amount_ = 0;

				std::string err_msg;
				result_ = ContractManager::Instance().Execute(Contract::TYPE_V8, parameter, true);
			}*/

		} while (false);
	}

	void OperationFrm::IssueAsset(std::shared_ptr<Environment> environment) {

		const protocol::OperationIssueAsset& ope = operation_.issue_asset();
		do {
			protocol::AssetStore asset_e;
			protocol::AssetKey key;
			key.set_issuer(source_account_->GetAccountAddress());
			key.set_code(ope.code());
			key.set_type(protocol::AssetKey_Type_UNLIMIT);
			if (!source_account_->GetAsset(key, asset_e)) {
				protocol::AssetStore asset;
				asset.mutable_key()->CopyFrom(key);
				asset.set_amount(ope.amount());
				source_account_->SetAsset(asset);
			}
			else {
				int64_t amount = asset_e.amount() + ope.amount();
				if (amount < asset_e.amount() || amount < ope.amount())
				{
					result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_AMOUNT_TOO_LARGE);
					result_.set_desc(utils::String::Format("IssueAsset asset(%s:%s:%d) overflow(" FMT_I64 " " FMT_I64 ")", key.issuer().c_str(), key.code().c_str(), (int)key.type(), asset_e.amount(), ope.amount()));
					break;
				}
				asset_e.set_amount(amount);
				source_account_->SetAsset(asset_e);
			}

		} while (false);
	}

	void OperationFrm::Payment(std::shared_ptr<Environment> environment) {
		const protocol::OperationPayment& payment = operation_.payment();

		do {
			if ((protocol::AssetKey_Type)payment.asset().key().type() != protocol::AssetKey_Type_UNLIMIT && (protocol::AssetKey_Type)payment.asset().key().type() != protocol::AssetKey_Type_LIMIT){
				result_.set_code(protocol::ERRCODE_ASSET_INVALID);
				result_.set_desc(utils::String::Format("Payment asset type must be limit or unlimit"));
				break;
			}

			std::shared_ptr<AccountFrm> dest_account = nullptr;

			if (!environment->GetEntry(payment.dest_address(), dest_account)) {
				result_.set_code(protocol::ERRCODE_ACCOUNT_NOT_EXIST);
				result_.set_desc(utils::String::Format("Dest account(%s) not exist", payment.dest_address().c_str()));
				break;
			}

			if (payment.has_asset()){
				protocol::AssetStore asset_e;
				protocol::AssetKey key = payment.asset().key();
				if (!source_account_->GetAsset(key, asset_e)) {
					result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
					result_.set_desc(utils::String::Format("Asset(%s:%s:%d) not exist", key.issuer().c_str(), key.code().c_str(), (int)key.type()));
					break;
				}

				if (payment.asset().key().type() == protocol::AssetKey_Type_UNLIMIT ||
					(payment.asset().key().type() == protocol::AssetKey_Type_LIMIT && source_account_->GetAccountAddress() != payment.asset().key().issuer() && dest_account->GetAccountAddress() != payment.asset().key().issuer())){

					int64_t sender_amount = asset_e.amount() - asset_e.freezn_amount() - payment.asset().amount();
					if (sender_amount < 0) {
						result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
						result_.set_desc(utils::String::Format("Asset(%s:%s:%d) low reserve", key.issuer().c_str(), key.code().c_str(), (int)key.type()));
						break;
					}
					asset_e.set_amount(sender_amount);
					source_account_->SetAsset(asset_e);

					protocol::AssetStore dest_asset;
					if (!dest_account->GetAsset(key, dest_asset)) {
						dest_asset.mutable_key()->CopyFrom(key);
						dest_asset.set_amount(payment.asset().amount());
						dest_account->SetAsset(dest_asset);
					}
					else {
						int64_t receiver_amount = dest_asset.amount() + payment.asset().amount();
						if (receiver_amount < dest_asset.amount() || receiver_amount < payment.asset().amount())
						{
							result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_AMOUNT_TOO_LARGE);
							result_.set_desc(utils::String::Format("Payment asset(%s:%s:%d) overflow(" FMT_I64 " " FMT_I64 ")", key.issuer().c_str(), key.code().c_str(), (int)key.type(), dest_asset.amount(), payment.asset().amount()));
							break;
						}
						dest_asset.set_amount(receiver_amount);
						dest_account->SetAsset(dest_asset);
					}
				}
				else{
					if (source_account_->GetAccountAddress() == payment.asset().key().issuer()){
						int64_t left_amount = asset_e.property().max_supply() - asset_e.property().issued_amount() - payment.asset().amount();
						if (left_amount < 0) {
							result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
							result_.set_desc(utils::String::Format("Asset(%s:%s:%d) low reserve", key.issuer().c_str(), key.code().c_str(), (int)key.type()));
							break;
						}
						int64_t sender_amount = asset_e.property().issued_amount() + payment.asset().amount();
						asset_e.mutable_property()->set_issued_amount(sender_amount);
						source_account_->SetAsset(asset_e);

						protocol::AssetStore dest_asset;
						if (!dest_account->GetAsset(key, dest_asset)) {
							dest_asset.mutable_key()->CopyFrom(key);
							dest_asset.set_amount(payment.asset().amount());
							dest_account->SetAsset(dest_asset);
						}
						else {
							int64_t receiver_amount = dest_asset.amount() + payment.asset().amount();
							if (receiver_amount < dest_asset.amount() || receiver_amount < payment.asset().amount())
							{
								result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_AMOUNT_TOO_LARGE);
								result_.set_desc(utils::String::Format("Payment asset(%s:%s:%d) overflow(" FMT_I64 " " FMT_I64 ")", key.issuer().c_str(), key.code().c_str(), (int)key.type(), dest_asset.amount(), payment.asset().amount()));
								break;
							}
							dest_asset.set_amount(receiver_amount);
							dest_account->SetAsset(dest_asset);
						}

					}
					else if (dest_account->GetAccountAddress() == payment.asset().key().issuer()){
						int64_t sender_amount = asset_e.amount() - asset_e.freezn_amount() - payment.asset().amount();
						if (sender_amount < 0) {
							result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
							result_.set_desc(utils::String::Format("Asset(%s:%s:%d) low reserve", key.issuer().c_str(), key.code().c_str(), (int)key.type()));
							break;
						}
						asset_e.set_amount(sender_amount);
						source_account_->SetAsset(asset_e);

						protocol::AssetStore dest_asset;
						if (!dest_account->GetAsset(key, dest_asset)) {
							result_.set_code(protocol::ERRCODE_ASSET_INVALID);
							result_.set_desc(utils::String::Format("Asset(%s:%s:%d) not exist", key.issuer().c_str(), key.code().c_str(), (int)key.type()));
							break;
						}
						else {
							int64_t receiver_amount = dest_asset.property().issued_amount() + payment.asset().amount();
							if (receiver_amount < dest_asset.amount() || receiver_amount < payment.asset().amount() || receiver_amount > dest_asset.property().max_supply())
							{
								result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_AMOUNT_TOO_LARGE);
								result_.set_desc(utils::String::Format("Payment asset(%s:%s:%d) overflow(" FMT_I64 " " FMT_I64 ")", key.issuer().c_str(), key.code().c_str(), (int)key.type(), (int64_t)dest_asset.property().max_supply(), payment.asset().amount()));
								break;
							}
							int64_t issued_amount = dest_asset.property().issued_amount() - payment.asset().amount();
							if (issued_amount < 0){
								result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
								result_.set_desc(utils::String::Format("Asset(%s:%s:%d) low reserve", key.issuer().c_str(), key.code().c_str(), (int)key.type()));
								break;
							}
							dest_asset.mutable_property()->set_issued_amount(issued_amount);
							dest_account->SetAsset(dest_asset);
						}
					}
					else{
					}
				}
			}
			
			/*std::string javascript = dest_account->GetProtoAccount().contract().payload();
			if (!javascript.empty()){
				ContractParameter parameter;
				parameter.code_ = javascript;
				parameter.input_ = payment.input();
				parameter.this_address_ = payment.dest_address();
				parameter.sender_ = source_account_->GetAccountAddress();
				parameter.ope_index_ = index_;
				parameter.timestamp_ = transaction_->ledger_->value_->close_time();
				parameter.blocknumber_ = transaction_->ledger_->value_->ledger_seq();
				parameter.ledger_context_ = transaction_->ledger_->lpledger_context_;
				parameter.pay_asset_amount_ = payment.asset();

				result_ = ContractManager::Instance().Execute(Contract::TYPE_V8, parameter);
			}*/
		} while (false);
	}

	void OperationFrm::SetMetaData(std::shared_ptr<Environment> environment) {

		do {
			auto ope = operation_.set_metadata();
			std::string key = ope.key();
			protocol::KeyPair keypair_e ;
			int64_t version = ope.version();
			bool delete_flag = ope.delete_flag();
			if (delete_flag){
				if (source_account_->GetMetaData(key, keypair_e)){
					if (version != 0) {
						if (keypair_e.version() != version) {
							result_.set_code(protocol::ERRCODE_INVALID_DATAVERSION);
							result_.set_desc(utils::String::Format("Data version(" FMT_I64 ") not valid", version));
							break;
						}
					}
					if (key.empty()){
						result_.set_code(protocol::ERRCODE_INVALID_PARAMETER);
						result_.set_desc(utils::String::Format("Data key is empty,key(%s)", key.c_str()));
						break;
					}
					source_account_->DeleteMetaData(keypair_e);
				}
				else{
					result_.set_code(protocol::ERRCODE_NOT_EXIST);
					result_.set_desc(utils::String::Format("DeleteMetaData not exist key(%s)", key.c_str()));
					break;
				}
			}
			else{
				if (source_account_->GetMetaData(key, keypair_e)) {

					if (version != 0) {
						if (keypair_e.version() + 1 != version) {
							result_.set_code(protocol::ERRCODE_INVALID_DATAVERSION);
							result_.set_desc(utils::String::Format("Data version(" FMT_I64 ") not valid", version));
							break;
						}
					}

					keypair_e.set_version(keypair_e.version() + 1);
					keypair_e.set_value(ope.value());
					source_account_->SetMetaData(keypair_e);

				}
				else {
					if (version != 1 && version != 0) {
						result_.set_code(protocol::ERRCODE_INVALID_DATAVERSION);
						result_.set_desc(utils::String::Format("Data version(" FMT_I64 ") not valid", version));
						break;
					}
					protocol::KeyPair keypair;
					keypair.set_value(ope.value());
					keypair.set_key(ope.key());
					keypair.set_version(1);
					source_account_->SetMetaData(keypair);
				}
			}			
		} while (false);

	}

	void OperationFrm::SetSignerWeight(std::shared_ptr<Environment> environment) {
		const protocol::OperationSetSignerWeight &ope = operation_.set_signer_weight();
		do {

			if (ope.master_weight() >= 0) {
				source_account_->SetProtoMasterWeight(ope.master_weight());
			}

			for (int32_t i = 0; i < ope.signers_size(); i++) {

				int64_t weight = ope.signers(i).weight();
				source_account_->UpdateSigner(ope.signers(i).address(), weight);
			}

		} while (false);
	}

	void OperationFrm::SetThreshold(std::shared_ptr<Environment> environment) {
		const protocol::OperationSetThreshold &ope = operation_.set_threshold();
		std::shared_ptr<AccountFrm> source_account = nullptr;

		do {
			if (ope.tx_threshold() >= 0) {
				source_account_->SetProtoTxThreshold(ope.tx_threshold());
			}

			for (int32_t i = 0; i < ope.type_thresholds_size(); i++) {
				source_account_->UpdateTypeThreshold(ope.type_thresholds(i).type(),
					ope.type_thresholds(i).threshold());
			}
		} while (false);
	}

	void OperationFrm::PayCoin(std::shared_ptr<Environment> environment) {
		auto ope = operation_.pay_coin();
		std::string address = ope.dest_address();
		std::shared_ptr<AccountFrm> dest_account_ptr = nullptr;
		int64_t reserve_coin = LedgerManager::Instance().GetCurFeeConfig().base_reserve();
		do {
			protocol::Account& proto_source_account = source_account_->GetProtoAccount();
			if (proto_source_account.balance() < ope.amount() + reserve_coin) {
				result_.set_code(protocol::ERRCODE_ACCOUNT_LOW_RESERVE);
				result_.set_desc(utils::String::Format("Account(%s) balance(" FMT_I64 ") - base_reserve(" FMT_I64 ") not enough for pay (" FMT_I64 ") ",
					proto_source_account.address().c_str(),
					proto_source_account.balance(),
					reserve_coin,
					ope.amount()					
					));
				break;
			}

			if (!environment->GetEntry(address, dest_account_ptr)) {
				if (ope.amount() < reserve_coin) {
					result_.set_code(protocol::ERRCODE_ACCOUNT_INIT_LOW_RESERVE);
					result_.set_desc(utils::String::Format("Account(%s) init balance(" FMT_I64 ") not enough for reserve(" FMT_I64 ")",
						address.c_str(),
						ope.amount(),
						reserve_coin
						));
					break;
				}

				protocol::Account account;
				account.set_balance(0);
				account.mutable_priv()->set_master_weight(1);
				account.mutable_priv()->mutable_thresholds()->set_tx_threshold(1);
				account.set_address(ope.dest_address());
				dest_account_ptr = std::make_shared<AccountFrm>(account);
				environment->AddEntry(ope.dest_address(), dest_account_ptr);
			}
			protocol::Account& proto_dest_account = dest_account_ptr->GetProtoAccount();

			int64_t new_balance = proto_source_account.balance() - ope.amount();
			proto_source_account.set_balance(new_balance);
			proto_dest_account.set_balance(proto_dest_account.balance() + ope.amount());

			/*std::string javascript = dest_account_ptr->GetProtoAccount().contract().payload();
			if (!javascript.empty()) {

				ContractParameter parameter;
				parameter.code_ = javascript;
				parameter.input_ = ope.input();
				parameter.this_address_ = ope.dest_address();
				parameter.sender_ = source_account_->GetAccountAddress();
				parameter.ope_index_ = index_;
				parameter.timestamp_ = transaction_->ledger_->value_->close_time();
				parameter.blocknumber_ = transaction_->ledger_->value_->ledger_seq();
				parameter.ledger_context_ = transaction_->ledger_->lpledger_context_;
				parameter.pay_coin_amount_ = ope.amount();

				std::string err_msg;
				result_ = ContractManager::Instance().Execute(Contract::TYPE_V8, parameter);

			}*/
		} while (false);
	}

	void OperationFrm::Log(std::shared_ptr<Environment> environment) {}

	void OperationFrm::ProcessOrder(std::shared_ptr<Environment> environment){
		auto ope = operation_.process_order();

		do
		{
			Database& db = Storage::Instance().lite_db();
			if (!CheckOrderVaild(ope, environment))
				return;

			order_result_.set_op_index(index_);
			protocol::AssetKey const& sheep = ope.selling();
			protocol::AssetKey const& wheat = ope.buying();

			passive_ = false;
			bool creating_new_order = false;
			std::string order_id = ope.order_id();


			if (!order_id.empty()){
				sell_sheep_order_ = OrderFrame::LoadOrder(source_account_->GetAccountAddress(), order_id, db);

				if (!sell_sheep_order_){
					std::string error_desc = utils::String::Format("Account(%s) selling(%s:%s:%d) buying(%s:%s:%d) order id(" FMT_U64 ") not exist", 
						source_account_->GetAccountAddress().c_str(),sheep.issuer().c_str(), sheep.code().c_str(), (int)sheep.type(),
						wheat.issuer().c_str(), wheat.code().c_str(), (int)wheat.type(),order_id.c_str());
					result_.set_code(protocol::ERRCODE_ORDER_NOT_FOUNT);
					result_.set_desc(error_desc);
					LOG_ERROR("%s", error_desc.c_str());

					order_result_.set_code(protocol::ERRCODE_ORDER_NOT_FOUNT);
					return;
				}

				sell_sheep_order_->GetOrder().CopyFrom(BuildOrder(source_account_->GetAccountAddress(), utils::String::BinToHexString(transaction_->GetContentHash()), ope, sell_sheep_order_->GetFlags(), sell_sheep_order_->GetOperationIndex()));
				passive_ = sell_sheep_order_->GetFlags() & OrderFrame::PASSIVE_FLAG;
			}
			else{
				// create a new order. first match and then storage  if left
				creating_new_order = true;
				protocol::Order o = BuildOrder(source_account_->GetAccountAddress(), utils::String::BinToHexString(transaction_->GetContentHash()), ope, passive_ ? OrderFrame::PASSIVE_FLAG : 0, index_);
				sell_sheep_order_ = std::make_shared<OrderFrame>(o);
			}

			//本单最大卖
			int64_t max_sheep_send = sell_sheep_order_->GetAmount();
			//账号最大可卖
			int64_t max_amount_sheep_can_sell;
			order_result_.set_code(protocol::ERRCODE_SUCCESS);

			soci::transaction sql_tx(db.GetSession());

			if (ope.amount() == 0){//for cancel
				sell_sheep_order_->SetAmount(0);
			}
			else{

				if (sheep.type() == protocol::AssetKey_Type_SELF_COIN){
					max_amount_sheep_can_sell = source_account_->GetBalanceAboveReserve();
				}
				else{
					max_amount_sheep_can_sell = sheep_asset_.amount();
				}

				int64_t max_wheat_can_buy;//最大可买小麦
				if (wheat.type() == protocol::AssetKey_Type_SELF_COIN){
					max_wheat_can_buy = INT64_MAX;
				}
				else{
					max_wheat_can_buy = INT64_MAX;
				}

				protocol::Price const& sheepPrice = sell_sheep_order_->GetPrice();
				{
					int64_t max_sheep_based_wheat=0;
					if (!utils::bigDivide(max_sheep_based_wheat, max_wheat_can_buy, (int64_t)sheepPrice.d(),
						(int64_t)sheepPrice.n(), utils::Rounding::eRoundDown))
						max_sheep_based_wheat = INT64_MAX;

					if (max_amount_sheep_can_sell > max_sheep_based_wheat)
						max_amount_sheep_can_sell = max_sheep_based_wheat;
				}

				if (max_amount_sheep_can_sell < max_sheep_send)
					max_sheep_send = max_amount_sheep_can_sell;

				int64_t sheep_sent=0, wheat_received=0;

				protocol::Price max_wheat_price;
				max_wheat_price.set_n(sheepPrice.d());
				max_wheat_price.set_d(sheepPrice.n());

				std::string order_desc = sell_sheep_order_->ToString();
				LOG_INFO("%s max_sheep_send(" FMT_I64") max_wheat_can_buy(" FMT_I64 ")", order_desc.c_str(), max_sheep_send, max_wheat_can_buy);

				OrderExchange oe(transaction_->ledger_,environment);
				//执行撮合
				OrderExchange::ConvertResult r = oe.ConvertWithOrders(
					sheep, max_sheep_send, sheep_sent, wheat, max_wheat_can_buy,
					wheat_received, [this, &max_wheat_price](OrderFrame const& o) {
					if (o.GetOrderID() == sell_sheep_order_->GetOrderID()){
						// don't let the offer cross itself when updating it
						return OrderExchange::eSkip;
					}
					if ((passive_ && (o.GetPrice() >= max_wheat_price)) ||(o.GetPrice() > max_wheat_price)){
						return OrderExchange::eStop;
					}
					if (o.GetSellerID() == source_account_->GetAccountAddress()){
						// we are crossing our own offer
						result_.set_code(protocol::ERRCODE_ORDER_CROSS_SELF);
						order_result_.set_code(protocol::ERRCODE_ORDER_CROSS_SELF);
						return OrderExchange::eStop;//这为啥是stop，不应该是skip吗
					}
					return OrderExchange::eKeep;
				});

				LOG_INFO("%s match result: max_sheep_send(" FMT_I64") max_wheat_can_buy(" FMT_I64 ") sheep_sent(" FMT_I64") wheat_received(" FMT_I64 ") ", order_desc.c_str(), max_sheep_send, max_wheat_can_buy, sheep_sent, wheat_received);
				assert(sheep_sent >= 0);

				switch (r)
				{
				case OrderExchange::eOK:
				case OrderExchange::ePartial:
					break;
				case OrderExchange::eFilterStop:// ???????????????????????????????????????????????????????
					if (order_result_.code() != protocol::ERRCODE_SUCCESS){
						return;
					}
					break;
				}

				for (auto const& oatom : oe.GetOrderTrail()){
					order_result_.add_orders_claimed()->CopyFrom(oatom);
				}

				if (wheat_received > 0){
					//fee compulate
					int64_t fee = sell_sheep_order_->GetFee(wheat_received);
					int64_t wheat_received_after_fee = wheat_received - fee;
					//AccountFrm::PayMatchFee(environment,wheat,fee);

					// it's OK to use mSourceAccount, mWheatLineA and mSheepLineA
					// here as OfferExchange won't cross offers from source account
					if (wheat.type() == protocol::AssetKey_Type_SELF_COIN){
						if (!source_account_->AddBalance(wheat_received)){

							std::string error_desc = utils::String::Format("Account(%s) balance(" FMT_I64 ") add(" FMT_I64 ") overflow", source_account_->GetAccountAddress().c_str(), source_account_->GetAccountBalance(), wheat_received);
							// this would indicate a bug in OrderExchange
							PROCESS_EXIT("%s", error_desc.c_str());
						}
					}
					else{
						int64_t new_amount = wheat_asset_.amount() + wheat_received;
						if (new_amount < wheat_asset_.amount() || new_amount < wheat_received){
							std::string error_desc = utils::String::Format("Asset(%s:%s:%d) overflow(" FMT_I64 " " FMT_I64 ")", wheat_asset_.key().issuer().c_str(), wheat_asset_.key().code().c_str(), wheat_asset_.key().type(), wheat_asset_.amount(), wheat_received);
							/*result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_AMOUNT_TOO_LARGE);
							result_.set_desc(error_desc);
							order_result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_AMOUNT_TOO_LARGE);
							LOG_ERROR("%s", error_desc.c_str());*/

							// this would indicate a bug in OrderExchange
							PROCESS_EXIT("%s", error_desc.c_str());
							return;
						}
						wheat_asset_.set_amount(new_amount);
						source_account_->SetAsset(wheat_asset_);
					}

					if (sheep.type() == protocol::AssetKey_Type_SELF_COIN){
						int64_t reserve_coin = LedgerManager::Instance().GetCurFeeConfig().base_reserve();
						if (source_account_->GetAccountBalance() - sheep_sent < reserve_coin){
							std::string error_desc = utils::String::Format("Account(%s) balance(" FMT_I64 ") not enough for reserve(" FMT_I64 ")", 
								source_account_->GetAccountAddress().c_str(), source_account_->GetAccountBalance(), reserve_coin);

							/*result_.set_code(protocol::ERRCODE_ACCOUNT_LOW_RESERVE);
							result_.set_desc(error_desc);
							order_result_.set_code(protocol::ERRCODE_ACCOUNT_LOW_RESERVE);
							LOG_FATAL("%s", error_desc.c_str());*/

							// this would indicate a bug in OrderExchange
							PROCESS_EXIT("%s", error_desc.c_str());
							return;
						}
						source_account_->AddBalance(-sheep_sent);
					}
					else{
						int64_t new_amount = sheep_asset_.amount() - sheep_sent;
						if (new_amount < 0){
							std::string error_desc = utils::String::Format("Account(%s) asset(%s:%s:%d) low reserve,order sold more than asset(" FMT_I64 ":" FMT_I64 ")",
								source_account_->GetAccountAddress().c_str(), sheep.issuer().c_str(), sheep.code().c_str(), (int)sheep.type(),
								sheep_asset_.amount() , sheep_sent);

							/*result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
							result_.set_desc(error_desc);
							order_result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
							LOG_FATAL("%s", error_desc.c_str());*/

							// this would indicate a bug in OrderExchange
							PROCESS_EXIT("%s", error_desc.c_str());
							return;
						}
						sheep_asset_.set_amount(new_amount);
						source_account_->SetAsset(sheep_asset_);
					}
				}

				// recomputes the amount of sheep for sale
				sell_sheep_order_->SetAmount(max_sheep_send - sheep_sent);
			}

			//剩余单子数量有
			if (sell_sheep_order_->GetAmount() > 0){ 
				// we still have sheep to sell so leave an offer

				if (creating_new_order){
					std::string order_id_str = utils::generatId(transaction_->ledger_->GetClosingLedgerSeq(), transaction_->index_, index_);
					sell_sheep_order_->SetOrderID(order_id_str);
					order_result_.set_effect(protocol::OperationOrderResult_OrderEffectType_ORDER_CREATED);
					sell_sheep_order_->StoreAdd(transaction_->ledger_,db);//存储剩余数量的单
				}
				else{//改单
					//innerResult().success().offer.effect(MANAGE_OFFER_UPDATED);
					order_result_.set_effect(protocol::OperationOrderResult_OrderEffectType_ORDER_UPDATED);
					sell_sheep_order_->StoreChange(transaction_->ledger_, db);
				}
				order_result_.mutable_order()->CopyFrom(sell_sheep_order_->GetOrder());
			}
			else{//完全成交 或者 撤单
				//innerResult().success().offer.effect(MANAGE_OFFER_DELETED);
				order_result_.set_effect(protocol::OperationOrderResult_OrderEffectType_ORDER_DELETED);
				//撤单
				if (!creating_new_order)
					sell_sheep_order_->StoreDelete(db);
			}

			sql_tx.commit();

		} while (false);
	}

	bool OperationFrm::CheckOrderVaild(protocol::OperationProcessOrder const& ope, std::shared_ptr<Environment> environment){
		protocol::AssetKey const& sheep = ope.selling();
		protocol::AssetKey const& wheat = ope.buying();

		if (ope.amount() == 0)
			return true;

		if (sheep.type() != protocol::AssetKey_Type_SELF_COIN){
			if (!source_account_->GetAsset(sheep, sheep_asset_)){
				std::string error_desc = utils::String::Format("Account(%s) do not hold asset(%s:%s:%d)",source_account_->GetAccountAddress().c_str() ,sheep.issuer().c_str(), sheep.code().c_str(), (int)sheep.type());
				result_.set_code(protocol::ERRCODE_ASSET_INVALID);
				result_.set_desc(error_desc);
				LOG_ERROR("%s", error_desc.c_str());
				return false;
			}

			if (sheep_asset_.amount() == 0){
				std::string error_desc = utils::String::Format("Account(%s) hold asset(%s:%s:%d) witch amount is 0", source_account_->GetAccountAddress().c_str(), sheep.issuer().c_str(), sheep.code().c_str(), (int)sheep.type());
				result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
				result_.set_desc(error_desc);
				LOG_ERROR("%s", error_desc.c_str());
				return false;
			}
		}

		if (wheat.type() != protocol::AssetKey_Type_SELF_COIN){
			source_account_->GetAsset(wheat, wheat_asset_);
		}

		return true;
	}

	protocol::Order OperationFrm::BuildOrder(const std::string& account_address, const std::string& tx_hash, const protocol::OperationProcessOrder& op, uint32_t flags, int32_t op_index){
		protocol::Order o;
		o.set_seller_address(account_address);
		o.mutable_remain_order()->CopyFrom(op);
		o.set_flags(flags);
		o.set_tx_hash(tx_hash);
		o.set_op_index(op_index);
		return o;
	}

	void OperationFrm::RegisterAsset(std::shared_ptr<Environment> environment){

		const protocol::OperationRegisterAsset& ope = operation_.register_asset();
		do {
			protocol::AssetStore asset_e;
			protocol::AssetKey key;
			key.set_issuer(source_account_->GetAccountAddress());
			key.set_code(ope.code());
			key.set_type(protocol::AssetKey_Type_LIMIT);
			if (!source_account_->GetAsset(key, asset_e)) {
				protocol::AssetStore asset;
				asset.mutable_key()->CopyFrom(key);
				asset.mutable_property()->CopyFrom(ope.property());
				source_account_->SetAsset(asset);
			}
			else {
				result_.set_code(protocol::ERRCODE_ASSET_INVALID);
				result_.set_desc(utils::String::Format("Register asset(%s:%s:%d) repeat", key.issuer().c_str(), key.code().c_str(), (int)key.type()));
			}

		} while (false);
	}
	void OperationFrm::SetAssetFee(std::shared_ptr<Environment> environment){

		const protocol::OperationSetAssetFee &ope = operation_.set_asset_fee();
		do 
		{
			protocol::AssetStore asset;
			protocol::AssetKey key;
			key.set_issuer(source_account_->GetAccountAddress());
			key.set_code(ope.code());
			key.set_type(protocol::AssetKey_Type_LIMIT);
			if (!source_account_->GetAsset(key, asset)){
				result_.set_code(protocol::ERRCODE_ACCOUNT_ASSET_LOW_RESERVE);
				result_.set_desc(utils::String::Format("Asset(%s:%s:%d) not exist", key.issuer().c_str(), key.code().c_str(), (int)key.type()));
				break;
			}

			if (!asset.has_property()){
				result_.set_code(protocol::ERRCODE_ASSET_INVALID);
				result_.set_desc(utils::String::Format("Asset(%s:%s:%d) does not have property", key.issuer().c_str(), key.code().c_str(), (int)key.type()));
				break;
			}

			asset.mutable_property()->set_fee_percent(ope.fee());

		} while (false);
		
	}

	void OperationFrm::FreeznAsset(protocol::AssetStore& asset, const int64_t& amount){
		int64_t new_amount = asset.freezn_amount() + amount;
		asset.set_freezn_amount(new_amount);
	}

	void OperationFrm::UnfreeznAsset(protocol::AssetStore& asset, const int64_t& amount){
		int64_t new_amount = asset.freezn_amount() - amount;
		asset.set_freezn_amount(new_amount);
	}

	void OperationFrm::GetOrderResult(std::vector<protocol::OperationOrderResult>& order_operation_results){
		if (operation_.type() ==protocol::Operation_Type_PROCESS_ORDER)
			order_operation_results.push_back(order_result_);
	}
	
	void OperationFrm::OptFee(const protocol::Operation_Type type) {
		protocol::FeeConfig fee_config = LedgerManager::Instance().GetCurFeeConfig();
		switch (type) {
		case protocol::Operation_Type_UNKNOWN:
			break;
		case protocol::Operation_Type_CREATE_ACCOUNT:
			ope_fee_ = fee_config.create_account_fee();
			break;
		case protocol::Operation_Type_PAYMENT:
			ope_fee_ = fee_config.pay_fee();
			break;
		case protocol::Operation_Type_ISSUE_ASSET:
			ope_fee_ = fee_config.issue_asset_fee();
			break;
		case protocol::Operation_Type_SET_METADATA:
			ope_fee_ = fee_config.set_metadata_fee();
			break;
		case protocol::Operation_Type_SET_SIGNER_WEIGHT:
			ope_fee_ = fee_config.set_sigure_weight_fee();
			break;
		case protocol::Operation_Type_SET_THRESHOLD:
			ope_fee_ = fee_config.set_threshold_fee();
			break;
		case protocol::Operation_Type_PAY_COIN:
			ope_fee_ = fee_config.pay_coin_fee();
			break;
		case protocol::Operation_Type_PROCESS_ORDER:
			ope_fee_ = fee_config.process_order_fee();
			break;
		case protocol::Operation_Type_REGISTER_ASSET:
			
			break;
		case protocol::Operation_Type_SET_ASSET_FEE:

			break;
		case protocol::Operation_Type_Operation_Type_INT_MIN_SENTINEL_DO_NOT_USE_:
			break;
		case protocol::Operation_Type_Operation_Type_INT_MAX_SENTINEL_DO_NOT_USE_:
			break;
		default:
			break;
		}
	}
}


