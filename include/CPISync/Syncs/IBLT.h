/* This code is part of the CPISync project developed at Boston University.  Please see the README for use and references. */

//
// Created by Eliezer Pearl on 7/9/18.
// Based on iblt.cpp and iblt.h in https://github.com/mwcote/IBLT-Research.
//

#ifndef CPISYNCLIB_IBLT_H
#define CPISYNCLIB_IBLT_H

#include <vector>
#include <utility>
#include <string>
#include <NTL/ZZ.h>
#include <sstream>
#include <CPISync/Aux/Auxiliary.h>
#include <CPISync/Data/DataObject.h>

using std::vector;
using std::hash;
using std::string;
using std::stringstream;
using std::pair;
using namespace NTL;

// The number of hashes used per insert
const long N_HASH = 4;

// The number hash used to create the hash-check for each entry
const long N_HASHCHECK = 11;

// Large prime for modulus
const long int LARGE_PRIME = 982451653;

// Shorthand for the hash type
typedef unsigned long int hash_t;

/*
 * IBLT (Invertible Bloom Lookup Table) is a data-structure designed to add
 * probabilistic invertibility to the standard Bloom Filter data-structure.
 *
 * A complete description of the IBLT data-structure can be found in: 
 * Goodrich, Michael T., and Michael Mitzenmacher. "Invertible bloom lookup tables." 
 * arXiv preprint arXiv:1101.2245 (2011).
 */
class IBLT {
public:
    // Communicant needs to access the internal representation of an IBLT to send and receive it
    friend class Communicant;

    /**
     * Constructs an IBLT object with size relative to expectedNumEntries.
     * @param expectedNumEntries The expected amount of entries to be placed into the IBLT
     * @param _valueSize The size of the values being added, in bits
     * @param isMultiset Is the IBLT going to store multiset values, default is false
     */
    IBLT(size_t expectedNumEntries, size_t _valueSize, bool isMultiset = false);
    
    // default destructor
    ~IBLT();
    
    /**
     * Inserts a key-value pair to the IBLT.
     * This operation always succeeds.
     * @param key The key to be added
     * @param value The value to be added
     * @require The key must be distinct in the IBLT
     */
    void insert(ZZ key, ZZ value);
    
    /**
     * Erases a key-value pair from the IBLT.
     * This operation always succeeds.
     * @param key The key to be removed
     * @param value The value to be removed
     */
    void erase(ZZ key, ZZ value);
    
    /**
     * Produces the value s.t. (key, value) is in the IBLT.
     * This operation doesn't always succeed.
     * This operation is destructive, as entries must be "peeled" away in order to find an element, i.e.
     * entries with only one key-value pair are subtracted from the IBLT until (key, value) is found.
     * @param key The key corresponding to the value returned by this function
     * @param result The resulting value corresponding with the key, if found.
     * If not found, result will be set to 0. result is unchanged iff the operation returns false.
     * @return true iff the presence of the key could be determined
     */
    bool get(ZZ key, ZZ& result);

    /**
     * Produces a list of all the key-value pairs in the IBLT.
     * With a low, constant probability, only partial lists will be produced
     * Listing is destructive, as the same peeling technique used in the get method is used.
     * Will remove all key-value pairs from the IBLT that are listed.
     * @param positive All the elements that could be inserted.
     * @param negative All the elements that were removed without being inserted first.
     * @return true iff the operation has successfully recovered the entire list
     */
    bool listEntries(vector<pair<ZZ, ZZ>>& positive, vector<pair<ZZ, ZZ>>& negative);
    /**
     * Insert a set of elements into IBLT
     * Does not support multisets
     * @param tarSet target set to be added to IBLT
     * @param elemSize size of element in the set
     * @param expnChldSet expected number of elements in the target set
    */
    void insert(multiset<shared_ptr<DataObject>> tarSet, size_t elemSize, size_t expnChldSet);

    /**
     * Delete a set of elements from IBLT
     * Does not support multisets
     * @param tarSet the target set to be deleted
     * @param elemSize size of element in the chld set
     * @param expnChldSet expected number of elements in the target set
    */
    void erase(multiset<shared_ptr<DataObject>> tarSet, size_t elemSize, size_t expnChldSet);

    /**
     * Convert IBLT to a readable string
     * @return string
    */
    string toString() const;

    string debugPrint() const;

    /**
     * fill the hashTable with a string generated from IBLT.toString() function
     * @param inStr a readable ascii string generted from IBLT.toString() function
    */
    void reBuild(string &inStr);
    /**
     * Subtracts two IBLTs.
     * -= is destructive and assigns the resulting iblt to the lvalue, whereas - isn't. -= is more efficient than -
     * @param other The IBLT that will be subtracted from this IBLT
     * @require IBLT must have the same number of entries and the values must be of the same size
     */
    IBLT operator-(const IBLT& other) const;
    IBLT& operator-=(const IBLT& other);

    /**
     * @return the number of cells in the IBLT. Not necessarily equal to the expected number of entries
     */
    size_t size() const;

    /**
     * @return the size of a value stored in the IBLT.
     */
    size_t eltSize() const;

    vector<hash_t> hashes; /* vector for all hashes of sets */

    /**
     * set the value of multiset and associated function pointers for get, insert and listEntries
     * @param _isMultiset is IBLT used for multisets
     */
    void setMultiset(bool _isMultiset);

private:
    // local data

    // default constructor - no internal parameters are initialized
    IBLT();

    // function pointer to the helper insert function
    void (IBLT::*_insert)(long plusOrMinus, ZZ key, ZZ value);

    // Helper function for insert and erase with XOR implementation
    void _insertXOR(long plusOrMinus, ZZ key, ZZ value);

    // Helper function for insert and erase with modular sums implementation
    void _insertModular(long plusOrMinus, ZZ key, ZZ value);

    // Returns the kk-th unique hash of the zz that produced initial.
    static hash_t _hashK(const ZZ &item, long kk);
    static hash_t _hash(const hash_t& initial, long kk);
    static hash_t _setHash(multiset<shared_ptr<DataObject>> &tarSet);

    // function pointer to the helper get function
    bool (IBLT::*_get)(ZZ key, ZZ& value);

    // helper function for get with XOR sum implementation
    bool _getXOR(ZZ key, ZZ& result);

    // helper function for get with modular sum implementation
    bool _getModular(ZZ key, ZZ& result);

    // function pointer to the helper insert function
    bool (IBLT::*_listEntries)(vector<pair<ZZ, ZZ>>& positive, vector<pair<ZZ, ZZ>>& negative);

    // helper function for`listEntries` with XOR sum implementation
    bool _listEntriesXOR(vector<pair<ZZ, ZZ>>& positive, vector<pair<ZZ, ZZ>>& negative);

    // helper function for`listEntries` with modular sum implementation
    bool _listEntriesModular(vector<pair<ZZ, ZZ>>& positive, vector<pair<ZZ, ZZ>>& negative);

    /* Insert an IBLT together with a value into a bigger IBLT
    * @param chldIBLT the IBLT to be inserted
    * @param chldHash a value represent in the hash_t type
    * */
    void insert(IBLT &chldIBLT, hash_t &chldHash);

    /* Erase an IBLT together with a value into a bigger IBLT
    * @param chldIBLT the IBLT to be erased
    * @param chldHash a value represent in the hash_t type
    * */
    void erase(IBLT &chldIBLT, hash_t &chldHash);

    // Represents each entry in the iblt
    class HashTableEntry
    {
    public:
        // Net insertions and deletions that mapped to this cell
        long count;

        // The bitwise xor-sum of all keys mapped to this cell
        ZZ keySum;

        // The bitwise xor-sum of all keySum checksums at each allocation
        hash_t keyCheck;

        // The bitwise xor-sum of all values mapped to this cell
        ZZ valueSum;

        // Returns whether the entry contains just one insertion or deletion
        // for XOR sum IBLT
        bool isPureXOR() const;

        // Returns whether the entry contains just one insertion or deletion
        // for modular sum based IBLT
        bool isPureModular() const;

        // Returns whether the entry contains multiple insertions or deletions of only one key-value pair
        // for multiset (modular sum) based IBLT
        bool isMultiPure() const;

        // Returns whether the entry is empty
        bool empty() const;
    };

    // vector of all entries
    vector<HashTableEntry> hashTable;

    // the value size, in bits
    size_t valueSize;

    // is input data multiset flag
    bool isMultiset = false;
};

#endif //CPISYNCLIB_IBLT_H
