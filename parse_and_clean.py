import glob
import pdfplumber
import multiprocessing
from functools import partial
from timeit import default_timer as timer
import argparse
import os


def process_files(path, out_path, globlist):
    def not_within_bboxes(obj):
        """Check if the object is in any of the table's bbox."""
        def obj_in_bbox(_bbox):
            """See https://github.com/jsvine/pdfplumber/blob/stable/pdfplumber/table.py#L404"""
            v_mid = (obj["top"] + obj["bottom"]) / 2
            h_mid = (obj["x0"] + obj["x1"]) / 2
            x0, top, x1, bottom = _bbox
            return (h_mid >= x0) and (h_mid < x1) and (v_mid >= top) and (v_mid < bottom)

        return not any(obj_in_bbox(__bbox) for __bbox in bboxes)

    with open(globlist, 'rb') as fh:

        output_filename = fh.name.split('/')[-1]
        #output_path = path+'/'+fh.name.split('/')[-2]+'/'
        output_path = out_path

        try:
            with pdfplumber.open(fh.name) as pdf:
                no_of_pages = len(pdf.pages)
                # print(f"Number of pages for {output_filename} = {no_of_pages}")
                images = pdf.images
                for i in range(no_of_pages):
                    bboxes = []
                    page = pdf.pages[i]
                    tables = page.find_tables(table_settings={
                                "vertical_strategy": "lines_strict",
                                "horizontal_strategy": "lines_strict",
                                "explicit_vertical_lines": page.curves + page.edges,
                                "explicit_horizontal_lines": page.curves + page.edges,
                            })
                    bboxes = [
                        table.bbox
                        for table in tables
                    ]

                    for imageObj in images:
                        if imageObj['page_number'] == i+1:
                            bounding_box = (imageObj['x0'], imageObj['top'], imageObj['x1'], imageObj['bottom'])
                            bboxes.append(bounding_box)
#                     outfile = open('output/'+str(output_filename)+'_onlyText.txt','a')

                    outfilename = os.path.join(output_path, output_filename[:-4]) +".txt"
                    outfile = open(outfilename, "a")

                    # if os.path.exists(outfilename):
                    #     print(f"File {outfilename} already exists. Skipping it")
                    #     continue

                    outfile.write(str(page.filter(not_within_bboxes).extract_text())+'\n')
                print("Done "+str(fh.name))
        except Exception as e:
            print("Unexpected error at"+str(fh.name)+ str(e))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    # pass the path to etd id
    parser.add_argument('--input-path', type=str, required=True)
    parser.add_argument('--output-path', type=str, required=True)

    args = parser.parse_args()

    start = timer()
    # path="/mnt/research-data/etdrepo/052"
    file_path = args.input_path
    out_path = args.output_path
    print(file_path)
    print(out_path)
    glob_path = os.path.join(file_path, '*.pdf')
    glob_ = glob.glob(glob_path)  # (file_path+'/'+'*.pdf')
    print(glob_)
    print(glob_)
    multiprocessing.freeze_support()
    process_pool = multiprocessing.Pool()
    func = partial(process_files, file_path, out_path)
    process_pool.map(func, glob_)
    process_pool.close()
    process_pool.join()
    print("That took " + str(timer()-start))
