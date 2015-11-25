/**
 * @file ibsbench.cxx
 * @brief Ibs benchmark/source
 */
#include <fcntl.h>
#include <getopt.h>
#include <unistd.h>
#include <iostream>
#include <chrono>
#include "ConfigFileReader.h"
#include "Controller.h"
#include "FileTools.h"
#include "Constants.h"

/* default values */
#define TEST_PREFIX "IBS_BENCH"
#define RANDOM_DEV "/dev/urandom"
#define N_IBP 4
#define RECORD_SIZE 4096
#define KEY_SIZE 160
#define NO_COMPRESSION 0
#define FRONT 1
#define BACK 2
#define hotDataOn true
#define hotDataOff false

using namespace ibs;

std::string MKTMPNAME(const size_t nbRamdomChar = 10) {
    std::vector<char> valid = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    valid.shrink_to_fit();
    std::string randomPart;
    for (size_t i = 0; i < nbRamdomChar; i++) {
        randomPart += valid[rand() % valid.size()];
    }
    std::string dir("/tmp/");
    std::string prefix(TEST_PREFIX);
    std::string name = dir + prefix + randomPart;
    return name;
}

void randomString(std::string& str, const int size, const int numberOfzeroToInsert = 10) {
    char* pt = new char[size];
    for (int i = 0; i < size; i++) {
        pt[i] = static_cast<char>(::rand() % 256);
    }
    str.assign(pt, size);
    delete[] pt;
    pt = NULL;
    // insert zero randomly
    for (int i = 0; i < numberOfzeroToInsert; i++) {
        size_t index = ::rand() % size;
        str[index] = 0;
    }
}

/*
 * Transform a map<string,string> into a string key=value and write it into a file
 */
void writeTestConfig(std::string fname, std::map<std::string, std::string> _cfg) {
    std::map<std::string, std::string>::iterator ite;
    std::ofstream f;
    f.open(fname.c_str());
    ite = _cfg.begin();
    while (ite != _cfg.end()) {
        f << ite->first << "=" << ite->second << std::endl;
        ++ite;
    }
    f.close();
}

void tokenizePath(const std::string& ibp_path, std::vector<std::string>& ibpPaths) {
    std::istringstream iss(ibp_path);
    std::string token;
    while (getline(iss, token, ',')) {
        ibpPaths.emplace_back(token);
    }
    ibpPaths.shrink_to_fit();
}

/* override parameters by taking them from configuration file */
struct OverrideParam {
        std::string ibp_path;
        std::string ibp_gen_path;
        std::string ibs_uuid;
        std::string ibs_owner;
};

/*
 * Configuration generator
 */
std::map<std::string, std::string> makeConfig(int n_ibp, bool hotData, int compression, struct OverrideParam* params) {
    std::string ibpGenPath;
    std::stringstream ss;
    std::vector < std::string > ibpDirs;
    std::map < std::string, std::string > cfg;
    ibpGenPath = std::string(MKTMPNAME());
    cfg[IBS_UUID] = params == NULL ? "ffd93fa8-5952-4943-a474-84bed12b0c9f" : params->ibs_uuid;
    cfg[IBS_OWNER] = params == NULL ? "aed12fbb-e76b-ae7d-4ff3-2b0c9f84bed1" : params->ibs_owner;
    cfg[IBP_GEN_PATH] = params == NULL ? ibpGenPath : params->ibp_gen_path;
    cfg[LOG_LEVEL] = "off";
    cfg[HOT_DATA] = hotData ? "yes" : "no";
    switch (compression) {
        case NO_COMPRESSION:
            cfg[COMPRESSION] = "no";
            break;
        case FRONT:
            cfg[COMPRESSION] = "front";
            break;
        case BACK:
            cfg[COMPRESSION] = "back";
            break;
        default:
            cfg[COMPRESSION] = "no";
            break;
    }
    for (int i = 0; i < n_ibp; i++) {
        ss.str("");
        ibpDirs.push_back(MKTMPNAME());
    }
    FileTools::removeDirectory(ibpGenPath);
    if (FileTools::createDirectory(ibpGenPath) == false) {
        /* TODO: handle exception */
    }
    ss.str("");
    if (params == NULL) {
        for (int i = 0; i < n_ibp; i++) {
            FileTools::removeDirectory (ibpDirs[i]);
            if (FileTools::createDirectory(ibpDirs[i]) == false) {
                /* TODO: handle exception */
            }
            ss << ibpDirs[i];
            if (i + 1 != n_ibp)
                ss << ",";
        }
        cfg[IBP_PATH] = ss.str();
    }
    else {
        const std::string ibp_path = params->ibp_path;
        std::vector < std::string > ibpPaths;
        tokenizePath(ibp_path, ibpPaths);
        for (auto& ibpDir : ibpPaths) {
            FileTools::removeDirectory(ibpDir);
            if (FileTools::createDirectory(ibpDir) == false) {
                /* TODO: handle exception */
            }
        }
        cfg[IBP_PATH] = ibp_path;
    }
    return cfg;
}

void tuneLevelDB(std::string configfile, int block_size, int block_restart_interval) {
    std::ofstream f;
    f.open(configfile.c_str(), std::ios_base::app);
    f << LDB_BLOCK_SIZE << "=" << block_size << std::endl;
    f << LDB_BLOCK_RESTART_INVERVAL << "=" << block_restart_interval << std::endl;
    f.close();
}

void dropCache() {
    sync();

    std::ofstream ofs("/proc/sys/vm/drop_caches");
    ofs << "3" << std::endl;
    ofs.flush();
    ofs.close();
}

void runBench(const int n_ibp, const int n_records, const int n_loops, const bool hotData, const int compression,
        const int ldb_block_size, const int ldb_block_restart_interval, struct OverrideParam* params) {
    std::string cfgFile(MKTMPNAME());
    std::string randomData;
    int randomFD = open(RANDOM_DEV, O_RDONLY);
    if (randomFD == -1) {
        std::cerr << "Could not open /dev/urandom ..." << std::endl;
        std::cerr << "Aborting ..." << std::endl;
        abort();
    }
    assert(randomFD != -1);
    char* key = new char[KEY_SIZE + 1];
    /* initialize IBS */
    writeTestConfig(cfgFile, makeConfig(n_ibp, hotData, compression, params));
    tuneLevelDB(cfgFile, ldb_block_size, ldb_block_restart_interval);
    AbstractController* ctrl = NULL;
    StatusCode st = Controller::create(cfgFile, ctrl);
    if (NULL == ctrl) {
        std::cerr << "Error : " << st.ToString() << std::endl;
        std::cerr << "Aborting ..." << std::endl;
        abort();
    }
    ctrl->start();

    randomString(randomData, RECORD_SIZE);

    double sumPut = 0.0;
    double sumGet = 0.0;
    for (int j = 0; j < n_loops; j++) {
        for (int i = 0; i < n_records; i++) {
            StatusCode st1;
            StatusCode st2;
            std::string keyStr;
            int readChar = 0;
            while (readChar != KEY_SIZE) {
                readChar += read(randomFD, key + readChar, KEY_SIZE - readChar);
            }
            assert(readChar == KEY_SIZE);
            keyStr.assign(key, KEY_SIZE);
            DataChunk keyChunk(keyStr);
            DataChunk dataChunk(randomData);

            std::chrono::time_point<std::chrono::system_clock> startPut, endPut, startGet, endGet;
            startPut = std::chrono::system_clock::now();
            st1 = ctrl->put(std::move(keyChunk), std::move(dataChunk));
            if (!st1.ok()) {
                std::cerr << "Error on write: " << st1.ToString() << std::endl;
                exit(-1);
            }
            endPut = std::chrono::system_clock::now();
            uint64_t elapsedPutTime = std::chrono::duration_cast < std::chrono::microseconds
                    > (endPut - startPut).count();

            startPut = std::chrono::system_clock::now();
            std::string buffer(RECORD_SIZE, 'A');
            DataChunk fetched(buffer);
            size_t count;
            st2 = ctrl->fetch(std::move(keyChunk), std::move(fetched), count);
            if (!st2.ok()) {
                std::cerr << "Error on get: " << st2.ToString() << std::endl;
                exit(-2);
            }
            endPut = std::chrono::system_clock::now();
            uint64_t elapsedGetTime = std::chrono::duration_cast < std::chrono::microseconds
                    > (endPut - startPut).count();

            sumPut += elapsedPutTime;
            sumGet += elapsedGetTime;
        }
    }
    double writeRate = (static_cast<double>(n_loops) * static_cast<double>(n_records));
    double readRate = (static_cast<double>(n_loops) * static_cast<double>(n_records));
    writeRate *= (static_cast<double>(RECORD_SIZE) / (1024.0 * 1024.0));
    readRate *= (static_cast<double>(RECORD_SIZE) / (1024.0 * 1024.0));
    assert(sumPut != 0.0);
    assert(sumGet != 0.0);
    writeRate /= (sumPut * 0.000001);
    readRate /= (sumGet * 0.000001);
    std::cout << "Results: ";
    std::cout << "Write rate = " << writeRate << " MiB/s, ";
    std::cout << "Read rate = " << readRate << " MiB/s" << std::endl;
    ctrl->stop();
    ctrl->destroy();
    delete ctrl;
    remove(cfgFile.c_str());
    delete[] key;
    close(randomFD);
}

std::string compresionCodeToString(int c) {
    std::string res;
    switch (c) {
        case NO_COMPRESSION:
            res = "NO COMPRESSION";
            break;
        case FRONT:
            res = "FRONT COMPRESSION";
            break;
        case BACK:
            res = "BACK COMPRESSION";
            break;
        default:
            res = "Wrong code.";
            break;
    }
    return res;
}

std::string hotDataToString(bool hotData) {
    if (hotData) {
        return "WITH HOT DATA";
    }
    else {
        return "WITHOUT HOT DATA";
    }
}

void printUsage(const std::string& progname) {
    std::cout << "Usage: " << progname;
    std::cout << "[ --configFile <FILE2> ]";
    std::cout << std::endl;
    std::cout << "The indicated FILE1 and FILE2 must exist and be valid" << std::endl;
    std::cout << std::endl;
    std::cout << "WARNING: Overriding default parameter with --configFile WILL REMOVE THE DIRECTORIES INDICATED"
            << std::endl;
    std::cout << "         FOR SECURITY REASON YOU SHOULD NOT RUN IT AS ROOT EITHER" << std::endl;
    std::cout << "         DON'T USE THIS IN A PRODUCTION ENVIRONMENT YOU MAY LOOSE DATA" << std::endl;
}

bool parseArgs(const int argc, const char* argv[], std::string& configFile) {
    bool isValid = true;
    bool loop = true;

    if ((argc == 1) || (argc == 2)) {
        while (loop) {
            int option_index = 0;
            static struct option long_options[] = { { "configFile", required_argument, 0, 'c' }, { 0, 0, 0, 0 } };
            int c = getopt_long(argc, static_cast<char* const *>(static_cast<void*>(argv)), "c:", long_options,
                    &option_index);

            /* Detect the end of the options. */
            if (c == -1)
                break;

            switch (c) {
                case 'c': {
                    configFile = optarg;
                    isValid = FileTools::isFile(configFile) && FileTools::isReadable(configFile);
                    if (!isValid) {
                        loop = false;
                    }
                    continue;
                    break; // break for switch only to silent compiler
                }
                case '?':
                    isValid = false; //to print usage
                    break;

                default:
                    break;
            }
        }
    }
    else {
        isValid = false;
    }
    return isValid;
}

void innerLoop(const int n_ibp, const int n_records, const int n_loops, const bool hotData, const int compression,
        const int ldb_block_size, const int ldb_block_restart_interval, struct OverrideParam& params,
        const bool isValidConfig) {
    std::cout << "benchmark hotData='" << hotDataToString(hotData);
    std::cout << "' compression='" << compresionCodeToString(compression);

    runBench(n_ibp, n_records, n_loops, hotData, compression, ldb_block_size, ldb_block_restart_interval,
            isValidConfig ? &params : NULL);
}

int main(const int argc, const char* argv[]) {
    std::string configFile;
    bool valid = parseArgs(argc, argv, configFile);
    if (!valid) {
        printUsage(argv[0]);
        return EXIT_FAILURE;
    }

    const int n_records = 10000; /* l records */
    const int n_loops = 10; /* repeat count */
    int n_ibp = N_IBP; /* default nb ibp */
    int ldb_block_size = RECORD_SIZE; /* default leveldb block value */
    int ldb_block_restart_interval = LDB_BLOCK_RESTART_INVERVAL_DEF; /* default leveldb block restart value */

    struct OverrideParam params;

    /* load configuration passed as argument if any ... */
    bool isValidConfig = !configFile.empty();
    std::unique_ptr<ConfigFileReader> config(new ConfigFileReader(configFile));
    if (isValidConfig) {
        if (config.get() != NULL) {
            config->read();
            isValidConfig = config->isLoaded();
        }
        else {
            isValidConfig = false;
        }
    }

    if (isValidConfig) {
        n_ibp = config->getInt(IBP_NB);
        ldb_block_size = config->getInt(LDB_BLOCK_SIZE);
        ldb_block_restart_interval = config->getInt(LDB_BLOCK_RESTART_INVERVAL);

        params.ibp_path = config->getString(IBP_PATH);
        params.ibp_gen_path = config->getString(IBP_GEN_PATH);
        params.ibs_uuid = config->getString(IBS_UUID);
        params.ibs_owner = config->getString(IBS_OWNER);
    }
    config.reset();

    double dataWrittenInMiB = (static_cast<double>(n_loops) * static_cast<double>(n_records));
    dataWrittenInMiB *= (static_cast<double>(RECORD_SIZE) / (1024.0 * 1024.0));

    std::cout << "=================================================" << std::endl;
    std::cout << "IBS benchmark :" << std::endl;
    std::cout << "=================================================" << std::endl;
    std::cout << "Config : " << std::endl;
    std::cout << " " << N_IBP << " IBP " << std::endl;
    std::cout << " " << n_loops << " loops of " << n_records << " records" << std::endl;
    std::cout << " " << "Record size = " << (RECORD_SIZE / 1024.0) << " KiB" << std::endl;
    std::cout << " Random data written by run : " << dataWrittenInMiB << " MiB" << std::endl << std::endl << std::endl;

    std::vector<bool> hotDataParams = { hotDataOn, hotDataOff };
    std::vector<int> compressionParams = { BACK, NO_COMPRESSION, FRONT };

    for (auto hotData : hotDataParams) {
        for (auto compression : compressionParams) {
            innerLoop(n_ibp, n_records, n_loops, hotData, compression, ldb_block_size, ldb_block_restart_interval,
                    params, isValidConfig);
        }
    }

    std::cout << "=================================================" << std::endl;

    return EXIT_SUCCESS;
}

