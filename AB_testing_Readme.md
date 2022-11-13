# AB-testing
Conducting AA testing, validating it using t-test, mann whitney test. Booststapping the data. 

<a href="https://github.com/egorkapot/Karpov-Analyst-Simulator/blob/main/AB%20testing.ipynb"> Working File </a>

<h1> Description of the project </h1>

The ML team came to me and made some new news feed recommendation algorithms. Naturally, it is expected that new algorithms will make users happier (that is, LTV will lengthen, money conversions will increase, etc.), and the product will be more convenient/pleasant to use. As an analyst, I must test this hypothesis.

The ML team told us "Recommendations make posts more interesting." Two new recommendation algorithms were made:

1) We show the user posts that are most similar to those that he liked.
2) We show the user posts that were liked by users similar to him.

We have 3 files created for each task. Tasks will be written below:

<h1> Task 1 </h1> 

So here's what to do: we have AA test data from '2022-07-06' to '2022-07-12'. What you need to do is simulate as if we ran 10,000 AA tests. At each iteration, you need to form non-repetitive subsamples of 500 users from the 2nd and 3rd experimental groups. Compare these subsamples with a t-test.

1. Construct a histogram of the distribution of the resulting 10000 p-values.

2. Calculate what percentage of p values turned out to be less than or equal to 0.05

3. Write a conclusion based on the conducted AA test, whether our splitting system works correctly.

4. Send a link to the merge request with the analysis.

<h1> Task 2 </h1> 

It's time to analyze the results of an experiment that we conducted with a team of data scientists. The experiment ran from 2022-07-13 to 2022-07-19 inclusive. For the experiment, 2 and 1 groups were involved.

Group 2 used one of the new post recommendation algorithms, group 1 was used as a control.

The main hypothesis is that the new algorithm in the 2nd group will lead to an increase in CTR.

Your job is to analyze the AB test data.

1. Choose an analysis method and compare CTR in two groups (we analyzed t-test, Poisson bootstrap, Mann-Whitney test, t-test on smoothed ctr (α=5) as well as t-test and Mann-Whitney test over bucket transform).
2. Compare the data with these tests. Also, look at the distributions with your eyes. Why did the tests work the way they did?
3. Describe a potential situation where such a change could occur. There is no perfect answer here.
4. Write a recommendation whether we will roll out the new algorithm to all new users or is it still not worth it.

<h1> Task 3 </h1> 

Relatively recently (in 2018), researchers from Yandex developed a cool method for analyzing tests on relationship metrics (just like ours) of the form 𝑥/𝑦 (We have
clicks/likes).

The idea of the method is as follows:

Instead of pushing “user” CTRs into the test, you can construct another metric and analyze it, but it is guaranteed (unlike the smoothed CTR) that if the test on this other metric “paints” and sees changes, then there are changes and in the original metric (that is, in likes per user and in user CTR

However, the method itself is very simple. What kind of metric is this?

1. Calculate the total CTR in the control group - 𝐶𝑇𝑅𝑐𝑜𝑛𝑡𝑟𝑜𝑙=𝑠𝑢𝑚(𝑙𝑖𝑘𝑒𝑠)/𝑠𝑢𝑚(𝑣𝑖𝑒𝑤𝑠)
2. We calculate in both groups Metric per User - 𝑙𝑖𝑛𝑒𝑎𝑟𝑖𝑧𝑒𝑑_𝑙𝑖𝑘𝑒𝑠 = 𝑙𝑖𝑘𝑒𝑠- ∗ <i>CTRcontrol</i> * 𝑣𝑖𝑒𝑤𝑠
3. Then we compare the differences in the groups by the t-test according to the metric

The method is simple, it is guaranteed that with a decent sample size (like ours, it will do), you can increase the sensitivity of your metric for free (or at least not make it worse). I think this is VERY cool.

A task:

1. Analyze the test between groups 0 and 3 on the metric of linearized likes. Is the difference visible? Has 𝑝−𝑣𝑎𝑙𝑢𝑒 gotten smaller?
2. Analyze the test between groups 1 and 2 on the metric of linearized likes. Is the difference visible? Has 𝑝−𝑣𝑎𝑙𝑢𝑒 gotten smaller?



