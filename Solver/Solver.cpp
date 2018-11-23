#include "Solver.h"

#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <mutex>

#include <cmath>


using namespace std;


namespace szx {

#pragma region Solver::Cli
int Solver::Cli::run(int argc, char * argv[]) {
    Log(LogSwitch::Szx::Cli) << "parse command line arguments." << endl;
    Set<String> switchSet;
    Map<String, char*> optionMap({ // use string as key to compare string contents instead of pointers.
        { InstancePathOption(), nullptr },
        { SolutionPathOption(), nullptr },
        { RandSeedOption(), nullptr },
        { TimeoutOption(), nullptr },
        { MaxIterOption(), nullptr },
        { JobNumOption(), nullptr },
        { RunIdOption(), nullptr },
        { EnvironmentPathOption(), nullptr },
        { ConfigPathOption(), nullptr },
        { LogPathOption(), nullptr }
    });

    for (int i = 1; i < argc; ++i) { // skip executable name.
        auto mapIter = optionMap.find(argv[i]);
        if (mapIter != optionMap.end()) { // option argument.
            mapIter->second = argv[++i];
        } else { // switch argument.
            switchSet.insert(argv[i]);
        }
    }

    Log(LogSwitch::Szx::Cli) << "execute commands." << endl;
    if (switchSet.find(HelpSwitch()) != switchSet.end()) {
        cout << HelpInfo() << endl;
    }

    if (switchSet.find(AuthorNameSwitch()) != switchSet.end()) {
        cout << AuthorName() << endl;
    }

    Solver::Environment env;
    env.load(optionMap);
    if (env.instPath.empty() || env.slnPath.empty()) { return -1; }

    Solver::Configuration cfg;
    cfg.load(env.cfgPath);

    Log(LogSwitch::Szx::Input) << "load instance " << env.instPath << " (seed=" << env.randSeed << ")." << endl;
    Problem::Input input;
    if (!input.load(env.instPath)) { return -1; }

    Solver solver(input, env, cfg);
    solver.solve();

    pb::Submission submission;
    submission.set_thread(to_string(env.jobNum));
    submission.set_instance(env.friendlyInstName());
    submission.set_duration(to_string(solver.timer.elapsedSeconds()) + "s");

    solver.output.save(env.slnPath, submission);
    #if SZX_DEBUG
    solver.output.save(env.solutionPathWithTime(), submission);
    solver.record();
    #endif // SZX_DEBUG

    return 0;
}
#pragma endregion Solver::Cli

#pragma region Solver::Environment
void Solver::Environment::load(const Map<String, char*> &optionMap) {
    char *str;

    str = optionMap.at(Cli::EnvironmentPathOption());
    if (str != nullptr) { loadWithoutCalibrate(str); }

    str = optionMap.at(Cli::InstancePathOption());
    if (str != nullptr) { instPath = str; }

    str = optionMap.at(Cli::SolutionPathOption());
    if (str != nullptr) { slnPath = str; }

    str = optionMap.at(Cli::RandSeedOption());
    if (str != nullptr) { randSeed = atoi(str); }

    str = optionMap.at(Cli::TimeoutOption());
    if (str != nullptr) { msTimeout = static_cast<Duration>(atof(str) * Timer::MillisecondsPerSecond); }

    str = optionMap.at(Cli::MaxIterOption());
    if (str != nullptr) { maxIter = atoi(str); }

    str = optionMap.at(Cli::JobNumOption());
    if (str != nullptr) { jobNum = atoi(str); }

    str = optionMap.at(Cli::RunIdOption());
    if (str != nullptr) { rid = str; }

    str = optionMap.at(Cli::ConfigPathOption());
    if (str != nullptr) { cfgPath = str; }

    str = optionMap.at(Cli::LogPathOption());
    if (str != nullptr) { logPath = str; }

    calibrate();
}

void Solver::Environment::load(const String &filePath) {
    loadWithoutCalibrate(filePath);
    calibrate();
}

void Solver::Environment::loadWithoutCalibrate(const String &filePath) {
    // EXTEND[szx][8]: load environment from file.
    // EXTEND[szx][8]: check file existence first.
}

void Solver::Environment::save(const String &filePath) const {
    // EXTEND[szx][8]: save environment to file.
}
void Solver::Environment::calibrate() {
    // adjust thread number.
    int threadNum = thread::hardware_concurrency();
    if ((jobNum <= 0) || (jobNum > threadNum)) { jobNum = threadNum; }

    // adjust timeout.
    msTimeout -= Environment::SaveSolutionTimeInMillisecond;
}
#pragma endregion Solver::Environment

#pragma region Solver::Configuration
void Solver::Configuration::load(const String &filePath) {
    // EXTEND[szx][5]: load configuration from file.
    // EXTEND[szx][8]: check file existence first.
}

void Solver::Configuration::save(const String &filePath) const {
    // EXTEND[szx][5]: save configuration to file.
}
#pragma endregion Solver::Configuration

#pragma region Solver
bool Solver::solve() {
    init();

    int workerNum = (max)(1, env.jobNum / cfg.threadNumPerWorker);
    cfg.threadNumPerWorker = env.jobNum / workerNum;
    List<Solution> solutions(workerNum, Solution(this));
    List<bool> success(workerNum);

    //Log(LogSwitch::Szx::Framework) << "launch " << workerNum << " workers." << endl;
    List<thread> threadList;
    threadList.reserve(workerNum);
    for (int i = 0; i < workerNum; ++i) {
        // TODO[szx][2]: as *this is captured by ref, the solver should support concurrency itself, i.e., data members should be read-only or independent for each worker.
        // OPTIMIZE[szx][3]: add a list to specify a series of algorithm to be used by each threads in sequence.
        threadList.emplace_back([&, i]() { success[i] = optimize(solutions[i], i); });
    }
    for (int i = 0; i < workerNum; ++i) { threadList.at(i).join(); }

   // Log(LogSwitch::Szx::Framework) << "collect best result among all workers." << endl;
    int bestIndex = -1;
    Length bestValue = Problem::MaxDistance;
    for (int i = 0; i < workerNum; ++i) {
        if (!success[i]) { continue; }
        //Log(LogSwitch::Szx::Framework) << "worker " << i << " got " << solutions[i].coverRadius << endl;
        if (solutions[i].coverRadius >= bestValue) { continue; }
        bestIndex = i;
        bestValue = solutions[i].coverRadius;
    }

    env.rid = to_string(bestIndex);
    if (bestIndex < 0) { return false; }
    output = solutions[bestIndex];
    return true;
}

void Solver::record() const {
    #if SZX_DEBUG
    int generation = 0;

    ostringstream log;

    System::MemoryUsage mu = System::peakMemoryUsage();

    Length obj = output.coverRadius;
    Length checkerObj = -1;
    bool feasible = check(checkerObj);

    // record basic information.
    log << env.friendlyLocalTime() << ","
        << env.rid << ","
        << env.instPath << ","
        << feasible << "," << (obj - checkerObj) << ",";
    if (Problem::isTopologicalGraph(input)) {
        log << obj << ",";
    } else {
        auto oldPrecision = log.precision();
        log.precision(2);
        log << fixed << setprecision(2) << (obj / aux.objScale) << ",";
        log.precision(oldPrecision);
    }
    log << timer.elapsedSeconds() << ","
        << mu.physicalMemory << "," << mu.virtualMemory << ","
        << env.randSeed << ","
        << cfg.toBriefStr() << ","
        << generation << "," << iteration << ",";

    // record solution vector.
    // EXTEND[szx][2]: save solution in log.
    log << endl;

    // append all text atomically.
    static mutex logFileMutex;
    lock_guard<mutex> logFileGuard(logFileMutex);

    ofstream logFile(env.logPath, ios::app);
    logFile.seekp(0, ios::end);
    if (logFile.tellp() <= 0) {
        logFile << "Time,ID,Instance,Feasible,ObjMatch,Distance,Duration,PhysMem,VirtMem,RandSeed,Config,Generation,Iteration,Solution" << endl;
    }
    logFile << log.str();
    logFile.close();
    #endif // SZX_DEBUG
}

bool Solver::check(Length &checkerObj) const {
    #if SZX_DEBUG
    enum CheckerFlag {
        IoError = 0x0,
        FormatError = 0x1,
        TooManyCentersError = 0x2
    };

    checkerObj = System::exec("Checker.exe " + env.instPath + " " + env.solutionPathWithTime());
    if (checkerObj > 0) { return true; }
    checkerObj = ~checkerObj;
    if (checkerObj == CheckerFlag::IoError) { Log(LogSwitch::Checker) << "IoError." << endl; }
    if (checkerObj & CheckerFlag::FormatError) { Log(LogSwitch::Checker) << "FormatError." << endl; }
    if (checkerObj & CheckerFlag::TooManyCentersError) { Log(LogSwitch::Checker) << "TooManyCentersError." << endl; }
    return false;
    #else
    checkerObj = 0;
    return true;
    #endif // SZX_DEBUG
}

void Solver::addFacility(const int f)
{	// 添加服务结点
	aux.S.insert(f);
	for (int v = 0; v < aux.nodeNum; ++v) {
		if (aux.G.at(f, v) < aux.D.at(0, v)) {
			aux.D.at(1, v) = aux.D.at(0, v);
			aux.F.at(1, v) = aux.F.at(0, v);
			aux.D.at(0, v) = aux.G.at(f, v);
			aux.F.at(0, v) = f;
		}
		else if (aux.G.at(f, v) < aux.D.at(1, v)) {
			aux.D.at(1, v) = aux.G.at(f, v);
			aux.F.at(1, v) = f;
		}
	}
	aux.scCur = *max_element(aux.D.begin(0), aux.D.end(0));
}

void Solver::findNext(const int v, const int f)
{	// 找次近服务结点，f为最近服务结点
	vector<int> distToServer;
	int tmpDist = Problem::MaxDistance;
	for (const auto &s : aux.S) {
		if (s != f) {
			if (aux.G.at(v, s) < tmpDist) {
				tmpDist = aux.G.at(v, s);
				distToServer.clear();
				distToServer.push_back(s);
			}
			else if (aux.G.at(v, s) == tmpDist) {
				distToServer.push_back(s);
			}
		}
	}
	auto ds = distToServer[rand.pick(distToServer.size())];
	aux.D.at(1, v) = tmpDist;
	aux.F.at(1, v) = ds;
}

void Solver::removeFacility(const int f)
{	// 删除一个服务结点
	aux.S.erase(f);
	for (int v = 0; v < aux.nodeNum; ++v) {
		if (f == aux.F.at(0, v)) {
			aux.D.at(0, v) = aux.D.at(1, v);
			aux.F.at(0, v) = aux.F.at(1, v);
			findNext(v, aux.F.at(0, v));
		}
		else if (f == aux.F.at(1, v)) {
			findNext(v, aux.F.at(0, v));
		}
	}
	aux.scCur = *max_element(aux.D.begin(0), aux.D.end(0));
}

void Solver::candidate()
{	// 找候选服务结点
	aux.candidates.clear();
	for (int i = 0; i < aux.nodeNum; ++i) {
		if (aux.D.at(0, i) == aux.scCur) {
			aux.candidates.push_back(i);
		}
	}
	int vertex = aux.candidates[rand.pick(aux.candidates.size())];
	aux.candidates.clear();

	int server = aux.F.at(0, vertex);
	auto index = find(aux.sortedG.begin(vertex), aux.sortedG.end(vertex), server);
	for (auto i = aux.sortedG.begin(vertex); i < index; ++i) {
		if (aux.S.find(*i) == aux.S.end()) {
			aux.candidates.push_back(*i);
		}
	}
}

void Solver::initialSol()
{
	int server = rand.pick(aux.nodeNum);
	addFacility(server);
	for (int i = 1; i < aux.centerNum; ++i) {
		candidate();
		server = aux.candidates[rand.pick(aux.candidates.size())];
		addFacility(server);
	}
	aux.scBest = aux.scCur;
}

szx::MvPair Solver::findPair(const int step)
{
	// 当前交换结果 禁忌最优结果 非禁忌最优结果
	unsigned int  newf, tabuf, noTabuf;
	newf = tabuf = noTabuf = Problem::MaxDistance;
	aux.tabuMv.clear(); aux.noTabuMv.clear();
	candidate();
	for (const auto &c : aux.candidates) {
		addFacility(c);
		for (const auto &f : aux.S) { aux.M[f] = 0; }
		for (const auto &v : aux.Nodes) {	// 更新M
			if (aux.D.at(1, v) > aux.M[aux.F.at(0, v)]) {
				aux.M[aux.F.at(0, v)] = aux.D.at(1, v);
			}
		}
		for (const auto &f : aux.S) {
			if (f != c) {	//除去刚加入结点
							// 当前交换产生的目标函数值
				newf = aux.scCur < aux.M[f] ? aux.M[f] : aux.scCur;
				if (aux.T.at(c, f) > step) {	// 当前交换被禁忌
					if (newf < tabuf) {
						tabuf = newf;
						aux.tabuMv.clear();
						aux.tabuMv.push_back({ c, f });
						//printf("tabu   =  (%d,%d),\tobj = %d\n", c, f, tabuf);
					}
					else if (newf == tabuf) {
						aux.tabuMv.push_back({ c, f });
						//printf("tabu  add:(%d,%d)\n", c, f);
					}
				}
				else {	// 当前交换不被禁忌
					if (newf < noTabuf) {
						noTabuf = newf;
						aux.noTabuMv.clear();
						aux.noTabuMv.push_back({ c, f });
						//printf("notabu   =  (%d,%d),\tobj = %d\n", c, f, noTabuf);
					}
					else if (newf == noTabuf) {
						aux.noTabuMv.push_back({ c, f });
						//printf("notabu  add:(%d,%d)\n", c, f);
					}
				}
			}
		}
		removeFacility(c);
	}
	// 解禁策略：禁忌队列结果优于历史最优目标值且优于非禁忌队列结果
	if (tabuf < aux.scBest && tabuf < noTabuf) {
		return aux.tabuMv[rand.pick(aux.tabuMv.size())];
	}
	else if (aux.noTabuMv.empty()) {
		//printf("noTabu list is null !!!");
		return aux.tabuMv[rand.pick(aux.tabuMv.size())];
	}
	return aux.noTabuMv[rand.pick(aux.noTabuMv.size())];
}

void Solver::init() {
	aux.nodeNum = input.graph().nodenum();
	aux.centerNum = input.centernum();
	aux.tt = 2 * aux.nodeNum + rand.pick(aux.centerNum);
	aux.G.init(aux.nodeNum, aux.nodeNum);
	fill(aux.G.begin(), aux.G.end(), Problem::MaxDistance);
	for (ID n = 0; n < aux.nodeNum; ++n) { aux.G.at(n, n) = 0; }

	if (Problem::isTopologicalGraph(input)) {
		aux.objScale = Problem::TopologicalGraphObjScale;
		for (auto e = input.graph().edges().begin(); e != input.graph().edges().end(); ++e) {
			// only record the last appearance of each edge.
			aux.G.at(e->source(), e->target()) = e->length();
			aux.G.at(e->target(), e->source()) = e->length();
		}
		Timer timer(30s);
		constexpr bool IsUndirectedGraph = true;
		IsUndirectedGraph
			? Floyd::findAllPairsPaths_symmetric(aux.G)
			: Floyd::findAllPairsPaths_asymmetric(aux.G);
		Log(LogSwitch::Preprocess) << "Floyd takes " << timer.elapsedSeconds() << " seconds." << endl;
	}
	else { // geometrical graph.
		aux.objScale = Problem::GeometricalGraphObjScale;
		for (ID n = 0; n < aux.nodeNum; ++n) {
			double nx = input.graph().nodes(n).x();
			double ny = input.graph().nodes(n).y();
			for (ID m = 0; m < aux.nodeNum; ++m) {
				if (n == m) { continue; }
				aux.G.at(n, m) = lround(aux.objScale * hypot(
					nx - input.graph().nodes(m).x(), ny - input.graph().nodes(m).y()));
			}
		}
	}
	aux.sortedG.init(aux.nodeNum, aux.nodeNum);
	for (ID i = 0; i < aux.nodeNum; ++i) {
		aux.dv.clear();
		for (ID j = 0; j < aux.nodeNum; ++j) {
			aux.dv.push_back({ aux.G.at(i,j),j });
		}
		sort(aux.dv.begin(), aux.dv.end());
		for (int k = 0; k < aux.nodeNum; ++k) {
			aux.sortedG.at(i, k) = aux.dv[k].second;
		}
	}

	aux.F.init(2, aux.nodeNum);
	fill(aux.F.begin(), aux.F.end(), aux.nodeNum);
	aux.D.init(2, aux.nodeNum);
	fill(aux.D.begin(), aux.D.end(), Problem::MaxDistance);
	aux.T.init(aux.nodeNum, aux.nodeNum);
	fill(aux.T.begin(), aux.T.end(), 0);
	aux.M.init(aux.nodeNum);
	aux.Nodes.init(aux.nodeNum);
	for (int k = 0; k < aux.nodeNum; ++k) { aux.Nodes[k] = k; }

	//auto pos = env.instPath.find("pmed");
	aux.optValue = aux.opt[stoi(env.instPath.substr(9, 2)) - 1];
}

bool Solver::optimize(Solution &sln, ID workerId) {
	//Log(LogSwitch::Szx::Framework) << "worker " << workerId << " starts." << endl;

	// reset solution state.
	bool status = true;
	auto &centers(*sln.mutable_centers());
	centers.Resize(aux.centerNum, Problem::InvalidId);

	// TODO[0]: replace the following random assignment with your own algorithm.
	initialSol();
	int iterTime = 0;
	while (!timer.isTimeOut()) {
		// 执行交换动作
		aux.m = findPair(iterTime);
		addFacility(aux.m.first);
		removeFacility(aux.m.second);
		aux.T.at(aux.m.first, aux.m.second) = aux.T.at(aux.m.second, aux.m.first) = aux.tt + iterTime;
		//cout << "Iter : " << iterTime << "add : " << aux.m.first << "\t remove : " << aux.m.second << endl;
		// 更新历史最优值
		if (aux.scCur < aux.scBest) {
			aux.scBest = aux.scCur;
			aux.bestS = aux.S;
			//printf("scBest = %d,\titer = %d\n", aux.scBest, iterTime);
		}
		// 找到最优值结束
		if (aux.scBest <= aux.optValue) break;
		++iterTime;
	}
	ID c = 0;
	for (const auto &s : aux.bestS) {
		centers[c++] = s;
	}
	//copy(aux.bestS.cbegin(), aux.bestS.cend(), centers);
	sln.coverRadius = aux.scBest; // record obj.

	//Log(LogSwitch::Szx::Framework) << "worker " << workerId << " ends." << endl;
	return status;
}
#pragma endregion Solver

}
