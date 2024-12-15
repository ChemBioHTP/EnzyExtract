from enzyextract.metrics.polaric import precision_recall, mean_log_relative_ratio, string_similarity, get_accuracy_score

def report_precision_recall(df):
    TP, FP, FN, wrong = precision_recall(df)
    print("TP:", TP.height)
    print("FP:", FP.height)
    print("FN:", FN.height)
    print("Wrong:", wrong.height)

    correct_rate = TP.height / (TP.height + wrong.height)

    print("Precision (assume no FP):", correct_rate)

    precision = TP.height / (TP.height + FP.height + wrong.height)
    recall = TP.height / (TP.height + FN.height)

    print("Precision:", precision)
    print("Recall:", recall)

    agree, total = get_accuracy_score(df, allow_brenda_missing=False)
    print(f"Runeem agreement score: ={agree}/{total} which is ({agree/total:.2f})")

    # calculate percent error
    kcat_error = mean_log_relative_ratio(df, 'kcat')
    km_error = mean_log_relative_ratio(df, 'km')
    print("kcat, Mean orders of magnitude error (perfect is 0):", kcat_error)
    print("kM, Mean orders of magnitude error (perfect is 0):", km_error)
    
    enzyme_similarity = string_similarity(df, 'enzyme')
    print("Enzyme similarity:", enzyme_similarity)
    substrate_similarity = string_similarity(df, 'substrate')
    print("Substrate similarity:", substrate_similarity)



def print_stats(stats):
    # pretty print stats
    for key, value in stats.items():
        print(f"{key}: {value}")