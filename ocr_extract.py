import boto3
import io
import os
import pandas as pd
from pdf2image import convert_from_path
from PIL import Image

class ocr_extract:

    data_dir = ""
    poppler_path = r""

    def __init__(self):
        # boto3 client
        self.client = boto3.client(
            'textract', 
            region_name='us-east-2', 
            aws_access_key_id='AKIAWHYXBOXDSYNX6A7I', 
            aws_secret_access_key='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'
        )


    """
    
    Convert the PDF to a series of images for extraction
    
    """
    def images(self, extract_path):
        # Store Pdf with convert_from_path function
        images = convert_from_path(
            pdf_path=extract_path,
            poppler_path=self.poppler_path,
            thread_count=10,
            dpi=500,
            grayscale=True,
            transparent=True,
            use_pdftocairo=True
        )
        return images


    """

    Helper function to map blocks used by the get_dataframe function

    """
    def map_blocks(self, blocks, block_type):
        return {
            block['Id']: block
            for block in blocks
            if block['BlockType'] == block_type
        }


    """

    Helper function to identify the child IDs used by the get_dataframe function

    """
    def get_children_ids(self, block):
        for rels in block.get('Relationships', []):
            if rels['Type'] == 'CHILD':
                yield from rels['Ids']


    """

    Get the table from textract and convert it into a 
    pandas dataframe

    """
    def get_dataframe(self, response, inv, inv_10b_Y, inv_10c_Y, inv_13_Y):

        blocks = response['Blocks']
        tables = self.map_blocks(blocks, 'TABLE')
        cells = self.map_blocks(blocks, 'CELL')
        words = self.map_blocks(blocks, 'WORD')
        selections = self.map_blocks(blocks, 'SELECTION_ELEMENT')

        i = 0
        index = 0
        y_diff = 0
        for table in tables:

            if inv == "10b":
                y_diff = tables[table]['Geometry']['Polygon'][0]['Y'] - inv_10b_Y
            if inv == "10c":
                y_diff = tables[table]['Geometry']['Polygon'][0]['Y'] - inv_10c_Y
            if inv == "13":
                y_diff = tables[table]['Geometry']['Polygon'][0]['Y'] - inv_13_Y

            if y_diff < 0.1:
                index = i
            i+=1
        
        table_index = 0
        for table in tables.values():

            # Determine all the cells that belong to this table
            table_cells = [cells[cell_id] for cell_id in self.get_children_ids(table)]

            # Determine the table's number of rows and columns
            n_rows = max(cell['RowIndex'] for cell in table_cells)
            n_cols = max(cell['ColumnIndex'] for cell in table_cells)
            content = [[None for _ in range(n_cols)] for _ in range(n_rows)]

            # Fill in each cell
            for cell in table_cells:
                cell_contents = [
                    words[child_id]['Text']
                    if child_id in words
                    else selections[child_id]['SelectionStatus']
                    for child_id in self.get_children_ids(cell)
                ]
                i = cell['RowIndex'] - 1
                j = cell['ColumnIndex'] - 1
                content[i][j] = ' '.join(cell_contents)

            if index == table_index:
                return pd.DataFrame(content[1:], columns=content[0])
            else:
                table_index+=1

        return 


    """

    Check if the previously identified table is investment related
    or does does not contain investment information and should be eliminated from output

    """
    def check_table(self, df):
        description = False
        value = False
        totals =  False
        isColumns = False
        
        if any(item.lower().find('description') !=-1 for item in df.columns):
            description = True
            isColumns = True

        if any(item.lower().find('value') !=-1 for item in df.columns):
            value = True

        if any(item.lower()=='fmv' for item in df.columns):
            value = True


        for index, row in df.iterrows():
            for x in df.columns:
                if str(row[x]).lower().find('description') != -1 and len(str(row[x])) == len("description"):
                    description = True
                    header_row = index

                if str(row[x]).lower().find('fair market value') != -1 and len(str(row[x])) == len("fair market value"):
                    value = True

                if str(row[x]).lower().find('fmv') != -1 and len(str(row[x])) == len("fmv"):
                    value = True

        
        if not isColumns:
            headers = df.iloc[header_row].values
            df.columns = headers

            for i in range(header_row +1):
                df.drop(index=i, axis=0, inplace=True)

        if description and value:
            return True
        else:
            return False

    """

    Create a CSV file containing the investment information identified 
    in the investment tables

    """
    def insert_csv(self, folder_path, investment_type, investment_arr):
        investment_file = folder_path + "investments_"+investment_type+".csv"
        investment_file_error = folder_path + "investments_"+investment_type+"_errors.csv"
        investment_file_df = pd.DataFrame()
        if investment_arr:
            for df in investment_arr:
                header_row = -1
                description = False
                value = False
                totals =  False
                isColumns = False
                
                if any(item.lower().find('description') !=-1 for item in df.columns):
                    description = True
                    isColumns = True

                if any(item.lower().find('value') !=-1 for item in df.columns):
                    value = True

                if any(item.lower()=='fmv' for item in df.columns):
                    value = True


                for index, row in df.iterrows():
                    for x in df.columns:
                        if str(row[x]).lower().find('description') != -1 and len(str(row[x])) == len("description"):
                            description = True
                            header_row = index

                        if str(row[x]).lower().find('fair market value') != -1 and len(str(row[x])) == len("fair market value"):
                            value = True

                        if str(row[x]).lower().find('fmv') != -1 and len(str(row[x])) == len("fmv"):
                            value = True

                
                if not isColumns and header_row != -1:
                    headers = df.iloc[header_row].values
                    df.columns = headers
                    for i in range(header_row +1):
                        df.drop(index=i, axis=0, inplace=True)

                try:
                    investment_file_df = pd.concat([investment_file_df, df], axis=0, join='outer', ignore_index=True, sort=False)
                except Exception as e:
                    df.to_csv(investment_file_error, sep=';',mode='a', index=False)
                    
        investment_file_df.to_csv(investment_file, sep=';',mode='a', index=False)


    """

    Identify the tables that were missed due to missing headers
    inbetween the detected table pages

    """
    def identify_missed_tables(self, folder_path, inv, investment_pages, inv_arr, error_file, images):
        if len(investment_pages) > 1:
            arr = investment_pages[::len(investment_pages)-1]
            first_ele = arr[0]
            last_ele = arr[1]

            for i in range(first_ele, last_ele):
                if i not in investment_pages:

                    # Save pages as images in the pdf
                    images[i].save('ein_img.png', 'PNG')

                    im = Image.open('ein_img.png')

                    buffered = io.BytesIO()
                    im.save(buffered, format='PNG')

                    # Call Amazon Textract
                    response = self.client.analyze_document(
                        Document={'Bytes': buffered.getvalue()},
                        FeatureTypes=['TABLES']
                    )

                    res = self.get_dataframe(response, inv, -1, -1, -1)
                    try:
                        for ele in res:
                            if self.check_table(ele):
                                investment_pages.append(i)
                            else:
                                res.to_csv(error_file, sep=';',mode='a', index=False)

                    except Exception as e:
                        print(e)
                    images[i].save(folder_path+'page_' + str(i)+'.png', 'PNG')
                    inv_arr.append(res)

        return inv_arr


    """

    Identify the tables detected based on keyword identifies

    """
    def identify_tables(self, buffered, inv, inv_arr, inv_10b_Y, inv_10c_Y, inv_13_Y, order ):
                    
        # Call Amazon Textract
        response = self.client.analyze_document(
            Document={'Bytes': buffered.getvalue()},
            FeatureTypes=['TABLES']
        )


        index = -1
        for i in range(len(order)):
            if order[i] == inv:
                index = i
                break
        if index != -1:
            res = self.get_dataframe(response, inv, inv_10b_Y, inv_10c_Y, inv_13_Y)
            inv_arr.append( res)
        return inv_arr
            
    """

    Detetct the pages that contain the relevant investment 
    tables

    """
    def extract_investments(self, folder_path, extract_path):
        investment_file_error = folder_path + "investments_errors.csv"

        inv_10b_arr = []
        inv_10c_arr = []
        inv_13_arr = []
        
        inv10b_pages = []
        inv10c_pages = []
        inv13_pages = []

        current = False

        images = self.images(extract_path)

        for i in range(len(images)):

            previous = current
            current = False

            inv_10b = False
            inv_10c = False
            inv_13 = False
            part_ii = True
            description = False
            value = False
            correct = True

            inv_10b_Y = 0
            inv_10c_Y = 0
            inv_13_Y = 0

            is_inv_10b = False
            is_inv_10c = False
            is_inv_13 = False

            order = []

            print("PAGE:\t" + str(i) + "/" + str(len(images)))

            # Save pages as images in the pdf
            images[i].save('ein_img.png', 'PNG')

            im = Image.open('ein_img.png')

            buffered = io.BytesIO()
            im.save(buffered, format='PNG')

            # Call Amazon Textract
            response = self.client.detect_document_text(
                Document={'Bytes': buffered.getvalue()}
            )                                


            # Print detected text
            for item in response["Blocks"]:
                if item["BlockType"] == "LINE":

                    if item["Text"].lower().find('description') != -1:
                        description = True
                    
                    if item["Text"].lower().find('investments') != -1:
                        description = True

                    if (item["Text"].lower().find('value') != -1 or item["Text"].lower().find('fmv') != -1):
                        value = True

                    if (item["Text"].lower().find('balance sheet') != -1):
                        correct = False

                    if (item["Text"].lower().find('see attach') != -1 or item["Text"].lower().find('see inv') != -1):
                        correct = False

            for item in response["Blocks"]:
                if item["BlockType"] == "LINE":
                    if (part_ii and description and value and correct) or previous:
                        if item["Text"].lower().find('line 10b') != -1 or item["Text"].lower().find('line10b') != -1 or item["Text"].lower().find('corporate stock') != -1 :
                            if item["Text"].lower().find('total') == -1 and not is_inv_10b:
                                inv_10b_Y = item["Geometry"]['Polygon'][0]['Y']
                                order.append("10b")

                            if correct and i not in inv10b_pages:
                                inv10b_pages.append(i)

                            images[i].save(folder_path+"page_"+str(i)+'.png', 'PNG')

                            inv_10b = True
                            current = True

                        if item["Text"].lower().find('line 10c') != -1 or item["Text"].lower().find('line10c') != -1 or item["Text"].lower().find('corporate bond') != -1 :
                            if item["Text"].lower().find('total') == -1 and not is_inv_10c:
                                inv_10c_Y = item["Geometry"]['Polygon'][0]['Y']
                                order.append("10c")

                            if correct and i not in inv10c_pages:
                                inv10c_pages.append(i)

                            images[i].save(folder_path+"page_"+str(i)+'.png', 'PNG')
                            
                            inv_10c = True
                            current = True

                        if item["Text"].lower().find('line 13') != -1 or item["Text"].lower().find('line13') != -1 or (
                                item["Text"].lower().find('investment') != -1 and item["Text"].lower().find('other') != -1):
                            if item["Text"].lower().find('total') == -1 and not is_inv_13:
                                inv_13_Y = item["Geometry"]['Polygon'][0]['Y']
                                order.append("13")

                            if correct and i not in inv13_pages:
                                inv13_pages.append(i)

                            images[i].save(folder_path+"page_"+str(i)+'.png', 'PNG')
                            
                            inv_13 = True
                            current = True

            for page in range(len(inv10b_pages)):
                if page + 1 < len(inv10b_pages):
                    if inv10b_pages[ page + 1] - inv10b_pages[page] > 5:
                        del inv10b_pages[page]
            
            for page in range(len(inv10c_pages)):
                if page + 1 < len(inv10c_pages):
                    if inv10c_pages[page + 1] - inv10c_pages[page] > 5:
                        del inv10c_pages[page]

            for page in range(len(inv13_pages)):
                if page + 1 < len(inv13_pages):
                    if inv13_pages[ page + 1] - inv13_pages[page] > 5:
                        del inv13_pages[page]

            if inv_10b:
                inv_10b_arr = self.identify_tables(buffered, "10b", inv_10b_arr ,inv_10b_Y, inv_10c_Y, inv_13_Y, order )
            if inv_10c:
                inv_10c_arr = self.identify_tables(buffered,"10c", inv_10c_arr ,inv_10b_Y, inv_10c_Y, inv_13_Y, order )
            if inv_13:
                inv_13_arr = self.identify_tables(buffered, "13",inv_13_arr ,inv_10b_Y, inv_10c_Y, inv_13_Y, order )

        inv_10b_arr = self.identify_missed_tables(folder_path, "10b",inv10b_pages, inv_10b_arr,investment_file_error, images)
        inv_10c_arr = self.identify_missed_tables(folder_path, "10c",inv10c_pages, inv_10c_arr, investment_file_error,images)
        inv_13_arr = self.identify_missed_tables(folder_path, "13",inv13_pages, inv_13_arr, investment_file_error,images)

        self.insert_csv(folder_path,"10b", inv_10b_arr)
        self.insert_csv(folder_path,"10c", inv_10c_arr)
        self.insert_csv(folder_path,"13", inv_13_arr)


if __name__ == '__main__':

    ocr = ocr_extract()

    ocr.data_dir = "data/"
    ocr.poppler_path = r"poppler-23.01.0/Library/bin"

    extract_dir = "pdfs/"

    for filename in os.listdir(extract_dir):

        ein_dir = filename.replace(".pdf", "")

        extract_path = os.path.join(extract_dir, filename) 
        
        # checking if it is a file
        if os.path.isfile(extract_path):
            print(extract_path)

            folder_path = ocr.data_dir + str(ein_dir) + "/"

            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
         
            ocr.extract_investments(folder_path, extract_path)