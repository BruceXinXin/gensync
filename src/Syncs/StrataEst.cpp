#include <CPISync/Syncs/StrataEst.h>


StrataEst::StrataEst(size_t value_size, size_t num_strata, size_t num_cells): numStrata(num_strata), numCells(num_cells), bits(value_size) {
//    numStrata = num_strata, numCells = num_cells, bits = value_size;
    for (int i = 0; i < numStrata; ++i) {
//        IBLT iblt = IBLT(numCells, bits);
//        Strata.push_back(iblt);
        Strata.emplace_back(numCells, bits);
    }
    if (!Strata.empty())
        numCells = Strata.back().size();

//    space = pow(2, numStrata);
    space = 1ul << numStrata;
}

StrataEst::StrataEst(const vector<IBLT> &myIBLT): numStrata(myIBLT.size()), numCells(myIBLT[0].size()), bits(myIBLT[0].eltSize())  {
    if (myIBLT.empty()) return;

//    numStrata = , numCells = , bits = ;
    for (int i = 0; i < numStrata; ++i) {
        Strata.push_back(myIBLT[i]);
    }
//    space = pow(2, numStrata);
    space = 1ul << numStrata;
}

void StrataEst::insert(shared_ptr<DataObject> &datum) {
    _insert(datum->to_ZZ());
}

void StrataEst::_insert(ZZ Elem) {
    // feed all element rough a hash function get their designated IBF
    // put the element into the designated IBF
    _inject(Elem, get_Assign_Ind(Elem));
}

vector<IBLT> StrataEst::getStrata() {
    return Strata;
}

int StrataEst::get_Assign_Ind(ZZ &Elem) {
    static std::hash<std::string> hasher;
    auto hashMod = hasher(toStr(Elem)) % space;
    if (hashMod == 0)
        return 0;
    return floor(log2(hashMod));
    // Core part: more chance to return higher int with highest int at 1/2 sample rate
    // hash fxn -> every time sample the same thing
    // Consider hash function spread out set elements evenly in the universe
    // biggest index returned is going to be the subspace from (space+1)/2 to (space)
}

void StrataEst::_inject(ZZ &item, int index) {
    auto& iblt = Strata[index];
    iblt.insert(item, item);
}

size_t StrataEst::estimate() {
    size_t count = 0;
    for (int i = 0; i < Strata.size(); ++i) {
        vector<pair<ZZ, ZZ>> pos, neg;
        bool success = Strata[numStrata - i - 1].listEntries(pos, neg);
        count += pos.size() + neg.size();

        if (success) {
            count *= (size_t) pow(2, i + 1);
            return count;
        }
    }
    Logger::error_and_quit("Set diff inestimable with current Strata "
                           "Estimation setting which could predict up to"
                           + to_string(pow(2, numStrata)) + "differences");
}

StrataEst &StrataEst::operator-=(const StrataEst &other) {
    if (numCells != other.numCells)
        Logger::error_and_quit("The Number of cells per IBF is not the same. We have "
                               + toStr(numCells) + " and They have " + toStr(other.numCells));

    if (numStrata != other.numStrata)
        Logger::error_and_quit("The Number of IBFs is not the same. We have "
                               + toStr(numStrata) + " and They have " + toStr(other.numStrata));

    if (bits != other.bits)
        Logger::error_and_quit("The Number of bits per element is not the same. We have "
                               + toStr(bits) + " and They have " + toStr(other.bits));

    for (int i = 0; i < numStrata; ++i) {
        this->Strata[i] -= other.Strata[i];
    }
    return *this;
}

size_t StrataEst::IBFsize() const { return numCells; }

size_t StrataEst::size() const { return numStrata; }

size_t StrataEst::eltSize() const { return bits; }
