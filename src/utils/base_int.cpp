#include "base_int.h"
#include <assert.h>

const uint128_t uint128_0(0);
const uint128_t uint128_1(1);
const uint128_t uint128_64(64);
const uint128_t uint128_128(128);

uint128_t::uint128_t(){
    UPPER = 0;
    LOWER = 0;
}

uint128_t::uint128_t(const uint128_t & rhs){
    UPPER = rhs.UPPER;
    LOWER = rhs.LOWER;
}

uint128_t uint128_t::operator=(const uint128_t & rhs){
    UPPER = rhs.UPPER;
    LOWER = rhs.LOWER;
    return *this;
}

uint128_t::operator bool() const{
    return ((UPPER | LOWER) != 0);
}

uint128_t::operator char() const{
    return (char) LOWER;
}
uint128_t::operator int() const{
    return (int) LOWER;
}

uint128_t::operator uint8_t() const{
    return (uint8_t) LOWER;
}

uint128_t::operator uint16_t() const{
    return (uint16_t) LOWER;
}

uint128_t::operator uint32_t() const{
    return (uint32_t) LOWER;
}

uint128_t::operator uint64_t() const{
    return (uint64_t) LOWER;
}

uint128_t uint128_t::operator&(const uint128_t & rhs) const{
return uint128_t(UPPER & rhs.UPPER, LOWER & rhs.LOWER);
}

uint128_t uint128_t::operator|(const uint128_t & rhs) const{
    return uint128_t(UPPER | rhs.UPPER, LOWER | rhs.LOWER);
}

uint128_t uint128_t::operator^(const uint128_t & rhs) const{
    return uint128_t(UPPER ^ rhs.UPPER, LOWER ^ rhs.LOWER);
}

uint128_t uint128_t::operator&=(const uint128_t & rhs){
    UPPER &= rhs.UPPER;
    LOWER &= rhs.LOWER;
    return *this;
}

uint128_t uint128_t::operator|=(const uint128_t & rhs){
    UPPER |= rhs.UPPER;
    LOWER |= rhs.LOWER;
    return *this;
}

uint128_t uint128_t::operator^=(const uint128_t & rhs){
    UPPER ^= rhs.UPPER;
    LOWER ^= rhs.LOWER;
    return *this;
}

uint128_t uint128_t::operator~() const{
    return uint128_t(~UPPER, ~LOWER);
}

uint128_t uint128_t::operator<<(const uint128_t & rhs) const{
    uint64_t shift = rhs.LOWER;
    if ((rhs.UPPER != 0) || (shift >= 128)){
        return uint128_0;
    }
    else if (shift == 64){
        return uint128_t(LOWER, 0);
    }
    else if (shift == 0){
        return *this;
    }
    else if (shift < 64){
        return uint128_t((UPPER << shift) + (LOWER >> (64 - shift)), LOWER << shift);
    }
    else if ((128 > shift) && (shift > 64)){
        return uint128_t(LOWER << (shift - 64), 0);
    }
    else{
        return uint128_0;
    }
}

uint128_t uint128_t::operator>>(const uint128_t & rhs) const{
    uint64_t shift = rhs.LOWER;
    if ((rhs.UPPER != 0) || (shift >= 128)){
        return uint128_0;
    }
    else if (shift == 64){
        return uint128_t(0, UPPER);
    }
    else if (shift == 0){
        return *this;
    }
    else if (shift < 64){
        return uint128_t(UPPER >> shift, (UPPER << (64 - shift)) + (LOWER >> shift));
    }
    else if ((128 > shift) && (shift > 64)){
        return uint128_t(0, (UPPER >> (shift - 64)));
    }
    else{
        return uint128_0;
    }
}

uint128_t uint128_t::operator<<=(const uint128_t & rhs){
    *this = *this << rhs;
    return *this;
}

uint128_t uint128_t::operator>>=(const uint128_t & rhs){
    *this = *this >> rhs;
    return *this;
}

bool uint128_t::operator!() const{
    return (UPPER | LOWER) == 0;
}

bool uint128_t::operator&&(const uint128_t & rhs) const{
    return ((bool) *this && rhs);
}

bool uint128_t::operator||(const uint128_t & rhs) const{
     return ((bool) *this || rhs);
}

bool uint128_t::operator==(const uint128_t & rhs) const{
    return ((UPPER == rhs.UPPER) && (LOWER == rhs.LOWER));
}

bool uint128_t::operator!=(const uint128_t & rhs) const{
    return ((UPPER != rhs.UPPER) | (LOWER != rhs.LOWER));
}

bool uint128_t::operator>(const uint128_t & rhs) const{
    if (UPPER == rhs.UPPER){
        return (LOWER > rhs.LOWER);
    }
    return (UPPER > rhs.UPPER);
}

bool uint128_t::operator<(const uint128_t & rhs) const{
    if (UPPER == rhs.UPPER){
        return (LOWER < rhs.LOWER);
    }
    return (UPPER < rhs.UPPER);
}

bool uint128_t::operator>=(const uint128_t & rhs) const{
    return ((*this > rhs) | (*this == rhs));
}

bool uint128_t::operator<=(const uint128_t & rhs) const{
    return ((*this < rhs) | (*this == rhs));
}

uint128_t uint128_t::operator+(const uint128_t & rhs) const{
    return uint128_t(UPPER + rhs.UPPER + ((LOWER + rhs.LOWER) < LOWER), LOWER + rhs.LOWER);
}

uint128_t uint128_t::operator+=(const uint128_t & rhs){
    UPPER = rhs.UPPER + UPPER + ((LOWER + rhs.LOWER) < LOWER);
    LOWER += rhs.LOWER;
    return *this;
}

uint128_t uint128_t::operator-(const uint128_t & rhs) const{
    return uint128_t(UPPER - rhs.UPPER - ((LOWER - rhs.LOWER) > LOWER), LOWER - rhs.LOWER);
}

uint128_t uint128_t::operator-=(const uint128_t & rhs){
    *this = *this - rhs;
    return *this;
}

uint128_t uint128_t::operator*(const uint128_t & rhs) const{
    // split values into 4 32-bit parts
    uint64_t top[4] ={UPPER >> 32, UPPER & 0xffffffff, LOWER >> 32, LOWER & 0xffffffff};
    uint64_t bottom[4] ={rhs.UPPER >> 32, rhs.UPPER & 0xffffffff, rhs.LOWER >> 32, rhs.LOWER & 0xffffffff};
    uint64_t products[4][4];

    for(int y = 3; y > -1; y--){
        for(int x = 3; x > -1; x--){
            products[3 - x][y] = top[x] * bottom[y];
        }
    }

    // initial row
    uint64_t fourth32 = products[0][3] & 0xffffffff;
    uint64_t third32 = (products[0][2] & 0xffffffff) + (products[0][3] >> 32);
    uint64_t second32 = (products[0][1] & 0xffffffff) + (products[0][2] >> 32);
    uint64_t first32 = (products[0][0] & 0xffffffff) + (products[0][1] >> 32);

    // second row
    third32 += products[1][3] & 0xffffffff;
    second32 += (products[1][2] & 0xffffffff) + (products[1][3] >> 32);
    first32 += (products[1][1] & 0xffffffff) + (products[1][2] >> 32);

    // third row
    second32 += products[2][3] & 0xffffffff;
    first32 += (products[2][2] & 0xffffffff) + (products[2][3] >> 32);

    // fourth row
    first32 += products[3][3] & 0xffffffff;

    // combines the values, taking care of carry over
    return uint128_t(first32 << 32, 0) + uint128_t(third32 >> 32, third32 << 32) + uint128_t(second32, 0) + uint128_t(fourth32);
}

uint128_t uint128_t::operator*=(const uint128_t & rhs){
    *this = *this * rhs;
    return *this;
}

std::pair <uint128_t, uint128_t> uint128_t::divmod(const uint128_t & lhs, const uint128_t & rhs) const{
    // Save some calculations /////////////////////
    if (rhs == uint128_0){
        throw std::runtime_error("Error: division or modulus by 0");
    }
    else if (rhs == uint128_1){
        return std::pair <uint128_t, uint128_t> (lhs, uint128_0);
    }
    else if (lhs == rhs){
        return std::pair <uint128_t, uint128_t> (uint128_1, uint128_0);
    }
    else if ((lhs == uint128_0) || (lhs < rhs)){
        return std::pair <uint128_t, uint128_t> (uint128_0, lhs);
    }

    std::pair <uint128_t, uint128_t> qr(uint128_0, lhs);
    uint128_t copyd = rhs << (lhs.bits() - rhs.bits());
    uint128_t adder = uint128_1 << (lhs.bits() - rhs.bits());
    if (copyd > qr.second){
        copyd >>= uint128_1;
        adder >>= uint128_1;
    }
    while (qr.second >= rhs){
        if (qr.second >= copyd){
            qr.second -= copyd;
            qr.first |= adder;
        }
        copyd >>= uint128_1;
        adder >>= uint128_1;
    }
    return qr;
}

uint128_t uint128_t::operator/(const uint128_t & rhs) const{
    return divmod(*this, rhs).first;
}

uint128_t uint128_t::operator/=(const uint128_t & rhs){
    *this = *this / rhs;
    return *this;
}

uint128_t uint128_t::operator%(const uint128_t & rhs) const{
    return *this - (rhs * (*this / rhs));
}

uint128_t uint128_t::operator%=(const uint128_t & rhs){
    *this = *this % rhs;
    return *this;
}

uint128_t uint128_t::operator++(){
    *this += uint128_1;
    return *this;
}

uint128_t uint128_t::operator++(int){
    uint128_t temp(*this);
    ++*this;
    return temp;
}

uint128_t uint128_t::operator--(){
    *this -= uint128_1;
    return *this;
}

uint128_t uint128_t::operator--(int){
    uint128_t temp(*this);
    --*this;
    return temp;
}

uint64_t uint128_t::upper() const{
    return UPPER;
}

uint64_t uint128_t::lower() const{
    return LOWER;
}

uint8_t uint128_t::bits() const{
    uint8_t out = 0;
    if (UPPER){
        out = 64;
        uint64_t up = UPPER;
        while (up){
            up >>= 1;
            out++;
        }
    }
    else{
        uint64_t low = LOWER;
        while (low){
            low >>= 1;
            out++;
        }
    }
    return out;
}

std::string uint128_t::str(uint8_t base, const unsigned int & len) const{
    if ((base < 2) || (base > 16)){
        throw std::invalid_argument("Base must be in th range 2-16");
    }
    std::string out = "";
    if (!(*this)){
        out = "0";
    }
    else{
        std::pair <uint128_t, uint128_t> qr(*this, uint128_0);
        do{
            qr = divmod(qr.first, uint128_t(base));
            out = "0123456789abcdef"[(uint8_t) qr.second] + out;
        } while (qr.first);
    }
    if (out.size() < len){
        out = std::string(len - out.size(), '0') + out;
    }
    return out;
}

std::ostream & operator<<(std::ostream & stream, const uint128_t & rhs){
    if (stream.flags() & stream.oct){
        stream << rhs.str(8);
    }
    else if (stream.flags() & stream.dec){
        stream << rhs.str(10);
    }
    else if (stream.flags() & stream.hex){
        stream << rhs.str(16);
    }
    return stream;
}

utils::uint256 utils::CryptoUint256(const std::string &input) {
	std::string str_out = utils::Sha256::Crypto(input);
	utils::uint256 tmp;
	tmp.init(str_out);
	return tmp;
}



int utils::hex_to_decimal(char a) {
	if (a <= '9' && a >= '0')	return a - '0';
	else if (a <= 'f' && a >= 'a')	return a - 'a' + 10;
	else if (a <= 'F' && a >= 'A')	return a - 'A' + 10;
	return  0;
}

bool utils::bigDivide(int64_t& result, int64_t A, int64_t B, int64_t C, Rounding rounding) {
	bool res;
	assert((A >= 0) && (B >= 0) && (C > 0));
	uint64_t r2;
	res = bigDivide(r2, (uint64_t)A, (uint64_t)B, (uint64_t)C, rounding);
	if (res) {
		res = r2 <= INT64_MAX;
		result = r2;
	}
	return res;
}

bool utils::bigDivide(uint64_t& result, uint64_t A, uint64_t B, uint64_t C, Rounding rounding) {
	// update when moving to (signed) int128
	uint128_t a(A);
	uint128_t b(B);
	uint128_t c(C);
	uint128_t x = rounding == Rounding::eRoundDown ? (a * b) / c : (a * b + c - 1) / c;

	result = (uint64_t)x;

	return (x <= UINT64_MAX);
}

int64_t  utils::bigDivide(int64_t A, int64_t B, int64_t C, Rounding rounding) {
	int64_t res;
	if (!bigDivide(res, A, B, C, rounding)) {
		throw std::overflow_error("overflow while performing bigDivide");
	}
	return res;
}

std::string utils::generatId(int64_t block_num, int32_t tx_index, int32_t op_index){
	assert(tx_index >= 0);
	assert(op_index >= 0);
	uint128_t a(block_num);
	a <<= 64;
	uint128_t b(tx_index+1);
	b <<= 32;
	uint128_t c(op_index+1);
	uint128_t d = a+b+c;
	return d.str(2, 128);//must be 2 for order sort
}

void utils::parseId(const std::string& id, int64_t& block_num, int32_t& tx_index, int32_t& op_index){
	std::string s_block_seq = id.substr(0, 64);
	std::string s_tx_index = id.substr(64, 32);
	std::string s_op_index = id.substr(96, 32);

	block_num = std::stoll(s_block_seq, nullptr, 2);
	assert(block_num > 0);
	tx_index = std::stoi(s_tx_index, nullptr, 2)-1;
	assert(tx_index >= 0);
	op_index = std::stoi(s_op_index, nullptr, 2)-1;
	assert(op_index >= 0);
}


