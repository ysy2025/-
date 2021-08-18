3 SparkSQL执行全过程概述
概述从sql->执行过程

3.1 SQL->RDD的案例
sparksql数据分析的过程:
初始化sparksession;sparksession正在取代sparkcontext成为入口
创建数据表,读取数据
通过sql进行数据分析

从sql到RDD,有两个阶段:logicalplan,physicalplan
sql->unresolved logicalplan -> analyzed plan -> Optimized plan -> iterator -> spark plan -> prepared spark plan

逻辑计划,将用户的sql->树形数据结构(逻辑算子树);sql语句中蕴含的逻辑,映射到逻辑算子树的不同节点
逻辑算子树的生成过程经历 3 个子阶段:未解析的逻辑算子树(Unresolved LogicalPlan,仅仅是数据结构,不包含任何数据信息等)
解析后的逻辑算子树(Analyzed LogicalPlan,节点中绑定各种信息)
优化后的逻辑算子树(OptimizedLogicalPlan,应用各种优化规则对一些低效的逻辑计划进行转换) 

物理计划阶段:逻辑算子树进行进一步转换,生成物理算子树.物理算子树的节点会直接生成 RDD 或对 RDD 进行 transformation 操作
首先,根据逻辑算子树,生成物理算子树的列表 Iterator[PhysicalPlan]
然后,从列表中按照一定的策略选取最优的物理算子树(SparkPlan);
最后,对选取的物理算子树进行提交前的准备工作

从sql解析到提交之前,都是在driver进行的,不涉及分布式环境
sparksession类的sql方法,调用sessionstate的各种对象,包括上述不同阶段对应的sparksqlparser,analyzer,optimizer,sparkplanner等,最后封装成queryexecution对象

3.2 catalyst引擎重要概念+数据结构
3.2.1 internalrow 体系
internalrow是用来表示一行行数据的类
3.2.3 Expression体系
不需要触发执行引擎就可以直接计算的单元;加减乘除,逻辑运算,转换操作,过滤操作等
treenode是框架,expression是灵魂
expression类包括5个方面:基本属性,核心操作,输入输出,字符串表示,等价性判断

3.3 内部数据类型系统

4.SparkSQL编译器 Parser
领域特定语言(Domain Specific Language,简称 DSL)
4.1 DSL之ANTLR
DSL模块涉及两个工作:
1,设计语法和语义,定义DSL中的元素
2,实现词法分析器Lexer,语法分析器Parser,完成DSL解析,转换为底层逻辑
http://icejoywoo.github.io/2019/01/16/intro-to-antlr4.html