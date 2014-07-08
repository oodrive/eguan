/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
/**
 * @file Controller.cpp
 * @brief The IBS controller/source
 */
#include "Controller.h"
#include "ConfigFileReader.h"
#include "FileTools.h"
#include "Constants.h"
#include "Recorder.h"
#include "Logger.h"
#include "IbsHandler.h"
#include <sstream>
#include <iterator>

namespace ibs {

// ----------------------------------------
// Utility fonctions
// ----------------------------------------

static Logger_t logger = ibs::Logger::getLogger("Controller");

bool areConfigEquals(ConfigFileReader& config1, ConfigFileReader& config2) {
    std::vector<std::string> unmodifiableKeys = { IBS_UUID, IBS_OWNER, COMPRESSION };
    bool isOK = true;
    for (auto key : unmodifiableKeys) {
        if (config1.getString(key) != config2.getString(key)) {
            LOG4IBS_WARN(logger,
                    key << " key mismatch: '" << config2.getString(key) << "' != '" << config1.getString(key) << "'");
            isOK = false;
        }
    }
    return isOK;
}

bool areConfigEquals(const std::string& configFile1, const std::string& configFile2) {
    // Allocate in smart pointer to be sure to release memory
    std::unique_ptr<ConfigFileReader> config1(new ConfigFileReader(configFile1));
    assert(config1.get() != NULL);
    config1->read();
    std::unique_ptr<ConfigFileReader> config2(new ConfigFileReader(configFile2));
    assert(config2.get() != NULL);
    config2->read();

    ConfigFileReader& config1Ref = *config1.get();
    ConfigFileReader& config2Ref = *config2.get();
    return areConfigEquals(config1Ref, config2Ref);
}

bool isKnownConfigurationKey(const std::string& key) {
    std::vector<std::string> allowedKeys = {
        HOT_DATA,
        IBP_GEN_PATH,
        IBP_PATH,
        IBS_UUID,
        IBS_OWNER,
        IBP_ID,
        IBP_NB,
        LOG_LEVEL,
        SYSLOG,
        BUFFER_ROTATION_THRESHOLD,
        BUFFER_ROTATION_DELAY,
        COMPRESSION,
        RECORD,
        DUMP_AT_STOP_BEST_EFFORT_DELAY,
        BUFFER_WRITE_DELAY_THRESHOLD,
        BUFFER_WRITE_DELAY_LEVEL_SIZE,
        BUFFER_WRITE_DELAY_INCR_MS,
        LDB_BLOCK_SIZE,
        LDB_BLOCK_RESTART_INVERVAL,
        LDB_DISABLE_BACKGROUND_COMPACTION_FOR_IBPGEN,
        INDICATED_RAM_SIZE};
    return std::find(allowedKeys.begin(), allowedKeys.end(), key) != allowedKeys.end();
}

bool findDuplicateEntry(const std::string& ibpgen_path, std::vector<std::string>& ibpPaths) {
    /* find duplicate entry */
    for (int j = (ibpPaths.size() - 1); j > 0; j--) {
        for (int i = j - 1; i >= 0; i--) {
            if (ibpPaths[i] == ibpPaths[j]) {
                LOG4IBS_WARN(logger, "Duplicate Ibp entry path='" << ibpPaths[j] << "'");
                return false;
            }
        }
    }
    for (int j = ibpPaths.size() - 1; j >= 0; j--) {
        if (ibpPaths[j] == ibpgen_path) {
            LOG4IBS_WARN(logger, "IbpGen path is present within the Ibp path list path='" << ibpPaths[j] << "'");
            return false;
        }
    }
    return true;
}

std::string ibpPathsToKey(const std::vector<std::string>& ibpPath) {
    /* rewrite ibp directory list in the configurator object */
    std::stringstream ss;
    std::string sep = ",";
    std::copy(ibpPath.begin(), ibpPath.end() - 1, std::ostream_iterator<std::string>(ss, sep.c_str()));
    ss << *ibpPath.rbegin();
    return ss.str();
}

bool tokenizeIbpPathKeyAndCheck(const std::string& ibp_path, std::vector<std::string>& ibpPaths) {
    std::istringstream iss(ibp_path);
    std::string token;
    /* If any of IBP directory is wrong => exit */
    while (getline(iss, token, ',')) {
        if (FileTools::dirOk(token)) {
            std::string absolute_path = FileTools::getAbsolutePath(token.c_str());
            if (absolute_path.empty()) {
                ibpPaths.shrink_to_fit();
                return false;
            }
            ibpPaths.push_back(absolute_path);
        }
        else {
            ibpPaths.shrink_to_fit();
            return false;
        }
    }
    ibpPaths.shrink_to_fit();
    return true;
}

// ----------------------------------------
// Configuration validation code
// ----------------------------------------

Controller::~Controller() {
    // first delete implementation
    pImpl.reset();
    // and then the configurator
    // used in the the implementation
    config.reset();
}

StatusCode Controller::create(const std::string& fname, AbstractController*& po_controller, bool checkRecorderOption) {
    return Controller::initCommon(fname, po_controller, true, checkRecorderOption);
}

StatusCode Controller::init(const std::string& fname, AbstractController*& po_controller, bool checkRecorderOption) {
    return Controller::initCommon(fname, po_controller, false, checkRecorderOption);
}

void Controller::initLog() {
    ibs::Logger::initialize_once();
    /* configure loglevel */
    std::string loglevel = this->config->getString(LOG_LEVEL);
    if (!loglevel.empty()) {
        ibs::Logger::setLevel(loglevel);
        LOG4IBS_DEBUG(logger, "loglevel='" << loglevel << "'");
    }/* else nothing, let the default loglevel as is */

    /* configure Syslog */
    std::string syslog = this->config->getString(SYSLOG);
    bool syslogEnabled = false;
    if (syslog == YES) {
        syslogEnabled = true;
        LOG4IBS_DEBUG(logger, "Syslog enabled");
    }
    else {
        syslogEnabled = false;
        LOG4IBS_DEBUG(logger, "Syslog disabled");
    }
    ibs::Logger::initialize_syslog(syslogEnabled);
}

StatusCode Controller::checkForKnownConfigurationKey() {
    auto list = this->config->getAllKeys();
    for (auto& key : list) {
        if (!isKnownConfigurationKey(key)) {
            return StatusCode::ConfigError();
        }
    }
    return StatusCode::OK();
}

StatusCode Controller::initCommon(const std::string& fname, AbstractController*& po_controller, bool isCreate,
        bool checkRecorderOption) {
    po_controller = NULL;
    Controller* ctrl = new Controller();
    // avoid to have the same message twice when using the recorder
    if (checkRecorderOption) {
        LOG4IBS_INFO(logger, "Initializing IBS controller. fname='" << fname << "'");
    }
    ctrl->configFile = fname;
    // test if configuration file is readable and build configurator object
    if (FileTools::isReadable(fname)) {
        ctrl->config = std::unique_ptr<ConfigFileReader>(new ConfigFileReader(fname));
        ctrl->config->read();

        // 1) init log
        ctrl->initLog();

        // 2) check for authorized value in config file
        StatusCode st1 = ctrl->checkForKnownConfigurationKey();
        if (!st1.ok()) {
            return st1;
        }

        // 3) second parse recorder option but
        // don't check if not said to avoid infinite loop
        if (checkRecorderOption) {
            std::string recorder_option = ctrl->config->getString(RECORD);
            if (!recorder_option.empty()) {
                LOG4IBS_INFO(logger, "Detected recorder file recorder_option='" << recorder_option << "'");
                if (FileTools::isFile(recorder_option) && FileTools::isWritable(recorder_option)) {
                    LOG4IBS_INFO(logger, "Controller changed to Recorder");
                    Recorder* recorder = NULL;
                    StatusCode s =
                            isCreate ? Recorder::create(fname, po_controller) : Recorder::init(fname, po_controller);
                    recorder = dynamic_cast<Recorder*>(po_controller);
                    if (recorder && s.ok()) {
                        // calculation of IBP NUMBER

                        std::vector<std::string> ibpPathList;
                        ctrl->config->getTokenizedString(IBP_PATH, ibpPathList);
                        size_t nb_ibp = ibpPathList.size();
                        ibpPathList.clear();
                        // IBS REPLAY needs to know the number of IBP
                        ctrl->config->setInt(IBP_NB, nb_ibp);
                        recorder->allocateRecordFile(recorder_option, ctrl->config.get());
                    }
                    delete ctrl; //avoid memory leak
                    return s;
                }
                else {
                    LOG4IBS_WARN(logger,
                            "Recorder file recorder_option='" << recorder_option << "' is not a writable file");
                    return StatusCode::ConfigError();
                }
            }
        }

        // 4) parse config and log related problems
        StatusCode st = ctrl->checkConfigCommon(isCreate);
        if (!st.ok()) {
            delete ctrl; //avoid memory leak
            return st;
        }
        if (ctrl->setUuid(ctrl->config->getString(IBS_UUID)) != 0) {
            LOG4IBS_WARN(logger, "Unable to parse Ibs UUID, uuid='" << ctrl->config->getString(IBS_UUID) << "'");
            delete ctrl; //avoid memory leak
            return StatusCode::ConfigError();
        }
        if (ctrl->setOwnerUuid(ctrl->config->getString(IBS_OWNER)) != 0) {
            LOG4IBS_WARN(logger, "Unable to parse owner UUID, uuid='" << ctrl->config->getString(IBS_OWNER) << "'");
            delete ctrl; //avoid memory leak
            return StatusCode::ConfigError();
        }

    }
    else { // abort
        LOG4IBS_ERROR(logger, "Unable to open config file. fname='" << fname << "'");
        delete ctrl; //avoid memory leak
        return StatusCode::ConfigError();
    }

    // 5) build all IBS objects
    assert(ctrl->config.get() != NULL);
    ctrl->pImpl.reset(new IbsHandler(*ctrl->config.get()));

    LOG4IBS_INFO(logger, "IBS Controller ready : uuid='" << ctrl->getUuid() << "'");
    po_controller = ctrl;
    return StatusCode::OK();
}

std::string Controller::getConfigFile() {
    return this->configFile;
}

std::string Controller::getUuid() {
    char* c_uuid = new char[64];
    uuid_unparse(this->uuid, c_uuid);
    std::string out(c_uuid);
    delete[] c_uuid;
    return out;
}

std::string Controller::getOwnerUuid() {
    char* c_uuid = new char[64];
    uuid_unparse(this->ownerUuid, c_uuid);
    std::string out(c_uuid);
    delete[] c_uuid;
    return out;
}

int Controller::setUuid(const std::string& str_uuid) {
    LOG4IBS_DEBUG(logger, "Setting uuid='" << str_uuid << "'");
    return uuid_parse(str_uuid.c_str(), this->uuid);
}

int Controller::setOwnerUuid(const std::string& str_uuid) {
    LOG4IBS_DEBUG(logger, "Setting ownerUuid='" << str_uuid << "'");
    return uuid_parse(str_uuid.c_str(), this->ownerUuid);
}

StatusCode Controller::checkConfigCommon(bool isCreating) {
    /* Reminder : configuration file structure
     * ibpgen_path=/some/dir
     * ibp_path=/dir1,/dir2
     * uuid=aa11-bb2244-cc34-1dd333
     * loglevel=warn
     */
    /* check ibpgen path validity */
    std::string ibpgen_path = this->config->getString(IBP_GEN_PATH);
    std::string absolute_ibpgen_path = FileTools::getAbsolutePath(ibpgen_path);
    const bool isOpenning = !isCreating;
    if (!absolute_ibpgen_path.empty()) {
        ibpgen_path = absolute_ibpgen_path;
    }
    else {
        LOG4IBS_DEBUG(logger, "Error while getting ibpgen absolute path='" << ibpgen_path << "'");
        return StatusCode::ConfigError();
    }

    /* test that base directory is there with sufficient rights */
    if (!FileTools::dirOk(ibpgen_path)) {
        LOG4IBS_ERROR(logger, "Ibs base directory is not valid path='" << ibpgen_path << "'");
        return StatusCode::ConfigError();
    }

    /* test that base directory is not empty when opening */
    if (isOpenning && FileTools::isDirectoryEmpty(ibpgen_path)) {
        LOG4IBS_ERROR(logger, "Ibs base directory is empty path='" << ibpgen_path << "'");
        return StatusCode::InitInEmptyDirectory();
    }
    /* ibpgen path has been checked and can be set in the configuration to write */
    this->config->setString(IBP_GEN_PATH, ibpgen_path);

    std::string ibp_path = this->config->getString(IBP_PATH);
    /* validate ibp directory list */
    std::vector<std::string> ibpPath;

    /* If any of IBP directory is wrong => exit */
    if (!tokenizeIbpPathKeyAndCheck(ibp_path, ibpPath))
        return StatusCode::ConfigError();
    /* If still no errors then check for duplicate entries */
    if (!findDuplicateEntry(ibpgen_path, ibpPath))
        return StatusCode::ConfigError();

    if (ibpPath.empty()) {
        LOG4IBS_WARN(logger,
                "No valid directory available for any IBP. The IBS will only rely on the Generational IBP.");
        return StatusCode::ConfigError();
    }
    this->config->setString(IBP_PATH, ibpPathsToKey(ibpPath));

    if (isOpenning) {
        if (!checkUuidOwnerAndCompression(ibpgen_path)) {
            return StatusCode::ConfigError();
        }
        if (!checkIbpPath(ibpgen_path, ibpPath)) {
            return StatusCode::ConfigError();
        }
    }

    // Check against existing or non-existing IBP
    std::map<int, std::string> newIbpPathes;

    std::string vold_owner_uuid = config->getString(IBS_OWNER);
    std::string ibs_uuid = config->getString(IBS_UUID);
    int newIbs_number = ibpPath.size();
    for (std::string path : ibpPath) {
        if (isCreating) {
            if (!FileTools::isDirectoryEmpty(path)) {
                return StatusCode::CreateInExistingIBS();
            }
        }
        else {
            std::ostringstream file_current;
            file_current << path << "/" << SIGNATURE_FILE;
            if (!FileTools::exists(file_current.str()) || FileTools::isDirectory(file_current.str())) {
                std::ostringstream message;
                message << "An IBP directory '" << path << "' does not contain a valid LevelDB.";
                LOG4IBS_ERROR(logger, message.str());
                if (FileTools::isDirectoryEmpty(path))
                    return StatusCode::InitInEmptyDirectory();
                else
                    return StatusCode::ConfigError();
            }

            std::unique_ptr<ConfigFileReader> pIbsSignature(new ConfigFileReader(file_current.str()));
            pIbsSignature->read();

            std::string owner_local_uuid = pIbsSignature->getString(IBS_OWNER);
            std::string ibs_local_uuid = pIbsSignature->getString(IBS_UUID);
            int ibs_id = pIbsSignature->getInt(IBP_ID);
            int save_ibs_number = pIbsSignature->getInt(IBP_NB);

            if (newIbs_number != save_ibs_number) {
                std::ostringstream message;
                message << "Ibs number " << newIbs_number;
                message << " should be " << save_ibs_number;
                message << " configuration not consistent with the existing one.";
                LOG4IBS_ERROR(logger, message.str());
                pIbsSignature.reset();
                return StatusCode::ConfigError();
            }

            if (0 == ibs_id || owner_local_uuid.empty() || ibs_local_uuid.empty()) {
                std::ostringstream message;
                std::string which_key(IBP_ID);
                if (owner_local_uuid.empty())
                    which_key = IBS_OWNER;
                if (ibs_local_uuid.empty())
                    which_key = IBS_UUID;
                message << "IBS signature file '" << file_current.str() << "' misses the key '" << which_key << "'.";
                LOG4IBS_ERROR(logger, message.str());
                pIbsSignature.reset();
                return StatusCode::ConfigError();
            }

            if (0 > ibs_id || ibs_id > save_ibs_number || 0 != vold_owner_uuid.compare(owner_local_uuid)
                    || 0 != ibs_uuid.compare(ibs_local_uuid)) {
                std::string errorMsgChunk("IBS id.");
                if (0 != vold_owner_uuid.compare(owner_local_uuid))
                    errorMsgChunk = "ibs owner uuid.";
                if (0 != ibs_uuid.compare(ibs_local_uuid))
                    errorMsgChunk = "ibs uuid.";

                std::ostringstream message;
                message << "IBS signature file '" << file_current.str() << "' has an invalid " << errorMsgChunk;
                LOG4IBS_ERROR(logger, message.str());
                pIbsSignature.reset();
                return StatusCode::ConfigError();
            }

            newIbpPathes[ibs_id] = path;
            pIbsSignature.reset();
        }
    }

    std::stringstream ss;
    if (isOpenning) {
        ss.str(std::string());
        auto it = newIbpPathes.begin();
        ss << it->second;
        ++it;
        while (it != newIbpPathes.end()) {
            ss << ',' << it->second;
            ++it;
        }
        this->config->setString(IBP_PATH, ss.str());
    }

    /* write cfg */
    ss.str(std::string());
    ss << ibpgen_path << "/" << CONFIG_FILE;
    this->config->write(ss.str());
    return StatusCode::OK();
}

bool Controller::checkUuidOwnerAndCompression(const std::string& ibpgen_path) {
    std::string old_ibpgen_config = ibpgen_path + "/" + CONFIG_FILE;

    std::unique_ptr<ConfigFileReader> cfg(new ConfigFileReader(old_ibpgen_config));
    assert(cfg.get() != NULL);
    cfg->read();
    assert(this->config.get() != NULL);
    ConfigFileReader& config1Ref = *this->config.get();
    ConfigFileReader& config2Ref = *cfg.get();
    return areConfigEquals(config1Ref, config2Ref);
}

bool Controller::checkIbpPath(const std::string& ibpgen_path, const std::vector<std::string>& ibp_paths) {
    std::string new_ibpgen_config = FileTools::getAbsolutePath(this->configFile);
    std::string old_ibpgen_config = FileTools::getAbsolutePath(ibpgen_path + "/" + CONFIG_FILE);

    // Bypass check when directory are renamed ...
    if (!new_ibpgen_config.empty())
        return true;

    // if the configuration filename is empty then no need to check
    if (old_ibpgen_config.empty())
        return false;

    // if the configuration filename didn't change then no need to check
    if (new_ibpgen_config == old_ibpgen_config)
        return true;

    std::unique_ptr<ConfigFileReader> cfg(new ConfigFileReader(old_ibpgen_config));
    cfg->read();
    std::string old_ipb_paths = cfg->getString(IBP_PATH);
    cfg.reset();

    // check ibp dirs
    std::vector<std::string> newIbps;
    std::vector<std::string> oldIbps;
    if (!tokenizeIbpPathKeyAndCheck(old_ipb_paths, oldIbps))
        return false;

    newIbps = std::vector<std::string>(ibp_paths.begin(), ibp_paths.end());
    // if the container doesn't have the same size the configuration isn't consistent.
    if (oldIbps.size() != newIbps.size()) {
        return false;
    }
    else {
        // sort element in same order
        std::sort(oldIbps.begin(), oldIbps.end());
        std::sort(newIbps.begin(), newIbps.end());
        // this way the == operator
        // can be used to compare
        return oldIbps == newIbps;
    }
}

// ----------------------------------------
// IBS control code
// ----------------------------------------

bool Controller::destroy() {
    if (this->pImpl.get() != NULL) {
        this->pImpl->destroy();
    }
    FileTools::removeFile(this->configFile);
    return true;
}

bool Controller::hotDataEnabled() {
    IbsHandler* ibsImpl = dynamic_cast<IbsHandler*>(this->pImpl.get());
    if (ibsImpl != NULL) {
        return ibsImpl->isHotDataEnabled();
    }
    else {
        return false;
    }
}

bool Controller::start() {
    LOG4IBS_INFO(logger, "Starting IBS controller.");
    assert(this->pImpl.get() != NULL);
    StatusCode status = this->pImpl->open();
    return status.ok();
}

bool Controller::stop() {
    LOG4IBS_INFO(logger, "Stopping IBS controller.");
    assert(this->pImpl.get() != NULL);
    this->pImpl->close();
    return true;
}

// ----------------------------------------
// IBS simple operations wrapper code
// ----------------------------------------

StatusCode Controller::fetch(const DataChunk&& key, DataChunk&& value, size_t& expected) {
    assert(this->pImpl.get() != NULL);
    std::string fetched;
    StatusCode st = this->pImpl->get(std::move(key), &fetched);
    if (st.ok()) {
        if (false == value.copyFrom(fetched)) {
            expected = fetched.size();
            return StatusCode::SliceTooSmall();
        }
    }
    return st;
}

StatusCode Controller::drop(const DataChunk&& key) {
    assert(this->pImpl.get() != NULL);
    return this->pImpl->drop(std::move(key));
}

StatusCode Controller::put(const DataChunk&& key, const DataChunk&& value) {
    assert(this->pImpl.get() != NULL);
    return this->pImpl->put(std::move(key), std::move(value));
}

StatusCode Controller::replace(const DataChunk&& oldKey, const DataChunk&& newKey, const DataChunk&& value) {
    assert(this->pImpl.get() != NULL);
    return this->pImpl->replace(std::move(oldKey), std::move(newKey), std::move(value));
}

// ----------------------------------------
// IBS transaction operations wrapper code
// ----------------------------------------

std::string Controller::createTransaction() {
    assert(this->pImpl.get() != NULL);
    return this->pImpl->createTransaction();
    return std::string();
}

void Controller::rollbackTransaction(const std::string& id) {
    assert(this->pImpl.get() != NULL);
    this->pImpl->rollbackTransaction(id);
}

StatusCode Controller::commitTransaction(const std::string& id) {
    assert(this->pImpl.get() != NULL);
    return this->pImpl->commitTransaction(id);
    return StatusCode::OK();
}

bool Controller::put(const std::string& id, const DataChunk&& key, const DataChunk&& value) {
    assert(this->pImpl.get() != NULL);
    return this->pImpl->put(id, std::move(key), std::move(value));
    return false;
}

bool Controller::replace(const std::string& id, const DataChunk&& oldKey, const DataChunk&& newKey,
        const DataChunk&& value) {
    assert(this->pImpl.get() != NULL);
    return this->pImpl->replace(id, std::move(oldKey), std::move(newKey), std::move(value));
    return false;
}

}/* namespace ibs */

void libibs_is_present() {
    return;
}
