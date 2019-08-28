#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <set>
#include <ctime>
#include <algorithm>
#include <Windows.h>

// 按键检测
#define KEY_DOWN(VK_NONAME) ((GetAsyncKeyState(VK_NONAME) & 0x8000) ? 1:0)

using namespace std;
typedef pair<unsigned int, unsigned int> MvPair;

const unsigned int INF = 64000;
unsigned int seed, N, E, P;			// 种子 结点数 边数 中心数
unsigned int scCur, scBest;			// 目标函数当前值与历史最优值
// G邻接矩阵 F最近与次近服务结点 D最短与次短服务边 T禁忌表
vector<vector<unsigned int>> G, F, D, T;
// M删除服务点产生的最长服务边  candidate候选服务结点
vector<unsigned int> M, candidates;
set<unsigned int> S, Nodes;			// 服务结点集 所有结点集
vector<MvPair> tabuMv, noTabuMv;	// 禁忌队列与非禁忌队列

string TimeStamp()
{	// 按格式返回当前时间
	char str[32]{};
	time_t a = time(nullptr);
	struct tm time_info;
	// localtime_s, Microsoft version (returns zero on success, an error code on failure)
	if (localtime_s(&time_info, &a) == 0) strftime(str, sizeof(str), "%Y-%m-%d %H:%M:%S", &time_info);
	return str;
}

void floydWarshall()
{	// 全最短路径Floyd算法
	for (int k = 0; k < N; ++k) {
		for (int i = 0; i < N; ++i) {
			for (int j = 0; j < N; ++j) {
				if (G[i][j] > G[i][k] + G[k][j]) {
					G[i][j] = G[i][k] + G[k][j];
				}
			}
		}
	}
}

template <typename T>
T reserSamp(const vector<T> &sampList)
{	//蓄水池抽样，暂时没用
	if (sampList.empty()) {
		return T();
	}
	T sample = sampList[0];
	for (int i = 1, j = 0; i < sampList.size(); ++i) {
		j = rand() % (i + 1);
		if (j < 1) {
			sample = sampList[i];
		}
	}
	return sample;
}

void readData(const string &filePath)
{	// 从txt读取数据
	ifstream readFile(filePath, ios::in);
	if (readFile) {
		readFile >> N >> E >> P;
		G = vector<vector<unsigned int>>(N, vector<unsigned int>(N, INF));
		for (int k = 0; k < N; ++k) {
			G[k][k] = 0;
		}
		int i = 0, j = 0, dist = 0;
		while (readFile >> i >> j >> dist) {
			if (dist < G[i - 1][j - 1]) {
				G[i - 1][j - 1] = G[j - 1][i - 1] = dist;
			}
		}
		floydWarshall();
		F = vector<vector<unsigned int>>(2, vector<unsigned int>(N, N));
		D = vector<vector<unsigned int>>(2, vector<unsigned int>(N, INF));
		T = vector<vector<unsigned int>>(N, vector<unsigned int>(N, 0));
		M = vector<unsigned int>(N, 0);

		Nodes.clear();
		for (int k = 0; k < N; ++k) {
			Nodes.insert(k);
		}
		S.clear();
	}
	else {
		cerr << "couldn't open: " << filePath << endl;
	}
}

template <class container>
void print(ostream& out, container &con)
{	// 打印顺序容器内容到指定流，支持基本类型
	for (typename container::iterator it = con.begin(); it != con.end(); ++it) {
		out << *it << ", ";
	}
	out << "\n" << endl;
}


void addFacility(const int f)
{	// 添加服务结点
	S.insert(f);
	for (int v = 0; v < N; ++v) {
		if (G[f][v] < D[0][v]) {
			D[1][v] = D[0][v];
			F[1][v] = F[0][v];
			D[0][v] = G[f][v];
			F[0][v] = f;
		}
		else if (G[f][v] < D[1][v]) {
			D[1][v] = G[f][v];
			F[1][v] = f;
		}
	}
	scCur = *max_element(D[0].cbegin(), D[0].cend());
}

void findNext(const int v, const int f)
{	// 找次近服务结点，f为最近服务结点
	vector<MvPair> distToServer;
	int tmpDist = INF;
	for (const auto &s : S) {
		if (s != f && G[s][v] <= tmpDist) {
			if (G[s][v] < tmpDist) {
				tmpDist = G[s][v];
				distToServer.clear();
				distToServer.push_back(make_pair(tmpDist, s));
			}
			else {
				distToServer.push_back(make_pair(tmpDist, s));
			}
		}
	}
	auto ds = distToServer[rand() % distToServer.size()];
	D[1][v] = ds.first;
	F[1][v] = ds.second;
}

void removeFacility(const int f)
{	// 删除一个服务结点
	S.erase(f);
	for (int v = 0; v < N; ++v) {
		if (f == F[0][v]) {
			D[0][v] = D[1][v];
			F[0][v] = F[1][v];
			findNext(v, F[0][v]);
		}
		else if (f == F[1][v]) {
			findNext(v, F[0][v]);
		}
	}
	scCur = *max_element(D[0].cbegin(), D[0].cend());
}

void candidate()
{	// 找候选服务结点
	candidates.clear();
	const int maxDist = scCur;
	for (auto i = D[0].cbegin(), j = i; i != D[0].cend(); ++i) {
		if (*i == maxDist) {
			candidates.push_back(i - j);
		}
	}
	int vertex = candidates[rand() % candidates.size()];
	candidates.clear();
	for (auto i = G[vertex].cbegin(), j = i; i != G[vertex].cend(); ++i) {
		// 候选服务结点不能在服务结点集中
		if (*i <= maxDist && S.find(i - j) == S.end()) {
			candidates.push_back(i - j);
		}
	}
}

void initial()
{
	// 每个算例一个随机种子
	//seed = time(NULL);
	//printf("seed = %d\n", seed);
	//srand(seed);
	int server = rand() % N, server1 = N;
	addFacility(server);
	for (int i = 1; i < P; ++i) {
		candidate();
		server1 = candidates[rand() % candidates.size()];
		addFacility(server1);
	}
	scBest = scCur;
}

MvPair findPair(const int step)
{
	// 当前交换结果 禁忌最优结果 非禁忌最优结果
	unsigned int  newf = INF, tabuf = INF, noTabuf = INF;
	tabuMv.clear(); noTabuMv.clear();
	for (const auto &c : candidates) {
		if (S.find(c) == S.end()) {	//再次确保不加入相同服务节点
			addFacility(c);
			for (const auto &f : S) {
				M[f] = 0;
			}
			for (const auto &v : Nodes) {	// 更新M
				if (D[1][v] > M[F[0][v]]) {
					M[F[0][v]] = D[1][v];
				}
			}
			for (const auto &f : S) {
				if (f != c) {	//除去刚加入结点
					// 当前交换产生的目标函数值
					newf = scCur < M[f] ? M[f] : scCur;
					if (T[c][f] > step) {	// 当前交换被禁忌
						if (newf < tabuf) {
							tabuf = newf;
							tabuMv.clear();
							tabuMv.push_back(make_pair(c, f));
							//printf("tabu   =  (%d,%d),\tobj = %d\n", c, f, tabuf);
						}
						else if (newf == tabuf) {
							tabuMv.push_back(make_pair(c, f));
							//printf("tabu  add:(%d,%d)\n", c, f);
						}
					}
					else {	// 当前交换不被禁忌
						if (newf < noTabuf) {
							noTabuf = newf;
							noTabuMv.clear();
							noTabuMv.push_back(make_pair(c, f));
							//printf("notabu   =  (%d,%d),\tobj = %d\n", c, f, noTabuf);
						}
						else if (newf == noTabuf) {
							noTabuMv.push_back(make_pair(c, f));
							//printf("notabu  add:(%d,%d)\n", c, f);
						}
					}
				}
			}
			removeFacility(c);
		}
	}
	// 解禁策略：禁忌队列结果优于历史最优目标值且优于非禁忌队列结果
	if ((tabuf < scBest && tabuf < noTabuf) || noTabuMv.empty()) {
		return tabuMv[rand() % tabuMv.size()];
	}
	return noTabuMv[rand() % noTabuMv.size()];
}
// 禁忌策略1
void tabuAct(const int a, const int b, const int tabuStep)
{
	for (auto &v : T[b]) {
		v = tabuStep;
	}
	for (int k = 0; k < N; ++k) {
		T[k][a] = tabuStep;
	}
}
// 禁忌策略2
void tabuAct(const int b, const int tabuStep)
{
	for (auto &v : T[b]) {
		v = tabuStep;
	}
}
// logistic函数
double logistic(const int iterTime) {
	return 1.0 / (1.0 + 3.0*exp(-5000.0*iterTime));//y(0)=0.25
}

int main()
{
	seed = time(NULL);	// 共用一个随机种子
	//seed = 1540698480;
	printf("seed = %d\n", seed);
	srand(seed);
	// 数据集路径
	string dataSetPath("D:\\MyFiles\\VSProjects\\PCenter\\data\\");
	// 数据集名称
	vector<string> insList = { "pmed01", "pmed02", "pmed03", "pmed04", "pmed05", "pmed06", "pmed07", "pmed08", "pmed09",
		"pmed10","pmed11", "pmed12", "pmed13", "pmed14", "pmed15", "pmed16", "pmed17", "pmed18", "pmed19", "pmed20",
		"pmed21", "pmed22", "pmed23", "pmed24", "pmed25", "pmed26", "pmed27", "pmed28", "pmed29", "pmed30",
		"pmed31", "pmed32", "pmed33", "pmed34", "pmed35", "pmed36", "pmed37", "pmed38", "pmed39", "pmed40" };
	vector<string> testInsList = { "pmed02", "pmed03", "pmed04",  "pmed19", "pmed20", "pmed30" };
	// 数据集对应最优值
	int opt[40] = { 127, 98, 93, 74, 48, 84, 64, 55, 37, 20, 59, 51,
		36, 26, 18, 47, 39, 28, 18, 13, 40, 38, 22, 15,11, 38, 32,
		18, 13, 9, 30, 29, 15, 11, 30, 27, 15, 29, 23, 13 };

	MvPair m;					// 交换对
	string insPath, dateTime;	// 某个数据集路径，运行该数据集时间
	int optIndex, tt;			// 最优值下标和禁忌长度

	for (const auto &insName : insList) {
		printf("%s\n", insName.c_str());				// 打印数据集名称
		insPath = dataSetPath + insName + ".txt";	// 构造数据集路径
		optIndex = stoi(insName.substr(4, 2)) - 1;	// 数据集最优值下标
		auto startTime = clock();	// 开始时间，用于记录程序运行时间
		dateTime = TimeStamp();		// 某数据集的运行时刻
		readData(insPath);
		initial();
		int iterTime = 1;
		bool terminate = false;		// 强制终止某算例运行，这时没达到最优值
		while (true) {
			//tt = static_cast<int>(logistic(iterTime)*N) + rand() % (P / 3 + 3) + iterTime / 3;
			//tt = P / 5 + 5 + rand() % ((N - P) / 2) + iterTime * P / N;
			//tt = P / 5 + 5 + rand() % (P / 10 + 5);
			//tt = P + rand() % P + N / 10;
			tt = static_cast<int>(0.6*N + rand() % static_cast<int>(0.8*P));
			//tt = P / 5 + 5 + rand() % static_cast<int>(P / 10 + 5);

			// 执行交换动作
			candidate();
			m = findPair(iterTime);
			addFacility(m.first);
			removeFacility(m.second);
			//tabuAct(m.first, m.second, tt + iterTime);
			//tabuAct(m.second, tt + iterTime);
			T[m.first][m.second] = T[m.second][m.first] = tt + iterTime;

			// 更新历史最优值
			if (scCur < scBest) {
				scBest = scCur;
				//printf("add %d，remove %d.\n", m.first, m.second);
				//printf("scBest = %d,\titer = %d\n", scBest, iterTime);
			}
			++iterTime;
			// 找到最优值结束
			if (scBest <= opt[optIndex]) break;
			if (KEY_DOWN('N')) {	// 按下N键运行下一个算例
				Sleep(1000);
				terminate = true;
				break;
			}
		}
		// 打印当前最优值以及迭代次数
		printf("scBest = %d,\titer = %d\n", scBest, iterTime);
		if (terminate) {	// 是否被终止
			printf("Terminate at %d.\n", iterTime);
		}
		// 记录运行时间
		auto endTime = clock();
		double runTime = (double)(endTime - startTime) / (double)CLOCKS_PER_SEC;
		printf("%.3f s\n", runTime);
		// 记录运行日志到文件
		ofstream out(".\\solution\\solution5.txt", ofstream::app);
		if (out) {
			if (terminate) {// 强制终止表示没找到最优解
				out << "[NOT FOUND] ";
			}
			out << "Run at: " << dateTime
				<< "\nIns: " << insName << "\t N = " << N << "\tP = " << P
				<< "\nSeed = " << seed << "\tSc = " << scBest
				<< "\niter times = " << iterTime << "\tRun time: " << runTime
				<< "\nS = ";
			print(out, S);	// 输出服务节点集
		}
		else {
			cerr << "couldn't open solution.txt" << endl;
		}
	}
	system("pause");
	return 0;
}