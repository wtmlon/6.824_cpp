#include <iostream>
#include <vector>
#include <cstring>

using namespace std;

struct kv_pair {
	string key;
	string value;
};

/*
 * 字符串分割，输入除了大小写字母就是空格
 */
vector<string> split(char* text, int len)
{
	vector<string> res;
	string tmp;
	for(int i=0; i<len; i++)
	{
		if((text[i] >= 'a'&&text[i]<='z') || (text[i]>='A' && text[i] <= 'Z'))
		{
			tmp += text[i];
		}
		else
		{
			if(tmp.length() != 0)
				res.push_back(tmp);
			tmp = "";
		}
	}
	if(tmp.length() != 0)
		res.push_back(tmp);

	return res;
}

/*
*	接受一个kv对，把value部分的文本进行split后存进新的kv数组里
*/
vector<kv_pair> map_f(kv_pair& inp)
{
	vector<kv_pair> res;
	int len = inp.value.length();
	char buff[len+1];
	strcpy(buff, inp.value.c_str());
	vector<string> sp = split(buff, len);
	for(const string& it: sp)
	{
		kv_pair tmp;
		tmp.key = it;
		tmp.value = "1";
		res.emplace_back(tmp);
	}
	return res;
}

/*
对特定的key进行reduce，统计某个key的出现次数
输入为某个key的所有kv_pair
*/
vector<string> reduce_f(vector<kv_pair>& inp)
{
	vector<string> res;
	for(auto& i: inp)
	{
		res.push_back(to_string(i.value.length()));
	}
	return res;
}