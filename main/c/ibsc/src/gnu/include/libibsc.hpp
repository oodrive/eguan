/**
 * @file libibs.hpp
 */
#ifndef LIBCIBS_HPP_
#define LIBCIBS_HPP_

#include "Controller.h"
#include <unordered_map>

extern "C" {
using namespace std;
using namespace ibs;
class IbsCBindings {
    public:
        /**
         * @brief Retrieve the IBS instance, thread safe implementing double checking mechanism.
         * @return The IBS instance pointer always different from NULL.
         */
        static IbsCBindings* getInstance();

        /**
         * @brief Create or init of an IBS instance, thread safe using exclusive lock.
         * @return The id of the created IBS instance or an error code.
         */
        int addIbp(const char* fname, bool create);

        IbsCBindings();
        virtual ~IbsCBindings();

        /**
         * @brief Retrieve an IBS instance, thread safe.
         * @param Id of the IBS to lookup for.
         * @return The IBS instance pointer if successful else NULL
         */
        AbstractController* getIbsById(int idToLookup);

        /**
         * @brief Save an association between an IBS instance and an Id, thread safe.
         * @param Pointer of the IBS instance to save.
         * @param Id of the IBS to save.
         * @return The id of the IBS instance pointer added.
         */
        int addIbsWithId(AbstractController* ibsToAdd);

        /**
         * @brief Remove an association IBS instance pointer givenSave an association between an IBS instance and an Id, thread safe.
         * WARNING: The instance is deleted here, don't delete it twice, don't forget to delete it !!!
         * @param Pointer of the IBS instance to save.
         * @param Id of the IBS to save.
         * @return Error code 0 on success
         */
        int deleteIbsById(const int id);
    private:
        typedef unordered_map<int, AbstractController*> idToIbsMap_t;
        idToIbsMap_t ctrlMap;
        int idx;
        static IbsCBindings *_instance;
        static pthread_mutex_t _singletonLock;
        static pthread_rwlock_t _mapProtectionLock;
};
}

#endif /* LIBIBS_HPP_ */
