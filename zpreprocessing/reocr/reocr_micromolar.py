import pandas as pd
import pymupdf
import os
from tqdm import tqdm
from enzyextract.utils.pmid_management import pmids_from_cache



import PIL
from PIL import Image as PILImage


import os
import random
import torch
import torchvision
import torchvision.transforms as transforms
from torch.utils.data import Dataset, DataLoader
from torchvision.models import resnet18, ResNet18_Weights
from PIL import Image
import polars as pl

### START use_resnet.py

# set the seed for reproducibility
torch.manual_seed(42)
random.seed(42)


# Custom dataset class
class CharacterDataset(Dataset):
    def __init__(self, m_folders, mu_folders, other_folders=None, transform=None, m_limits=None, mu_limits=None):
        self.images = []
        self.labels = []
        self.transform = transform
        
        # Load 'm' images
        for i, m_folder in enumerate(m_folders):
            selection = os.listdir(m_folder)
            if m_limits is not None:
                # shuffle and select m_limits[i]
                random.shuffle(selection)
                selection = selection[:m_limits[i]]
                
            for img_name in selection:
                if img_name.endswith(('.png', '.jpg', '.jpeg')):
                    self.images.append(os.path.join(m_folder, img_name))
                    self.labels.append(0)
        
        # Load 'mu' images
        for i, mu_folder in enumerate(mu_folders):
            selection = os.listdir(mu_folder)
            if mu_limits is not None:
                # shuffle and select mu_limits[i]
                random.shuffle(selection)
                selection = selection[:mu_limits[i]]
                
            for img_name in selection:
                if img_name.endswith(('.png', '.jpg', '.jpeg')):
                    self.images.append(os.path.join(mu_folder, img_name))
                    self.labels.append(1)
        
        for i, other_folder in enumerate(other_folders):
            selection = os.listdir(other_folder)
                
            for img_name in selection:
                if img_name.endswith(('.png', '.jpg', '.jpeg')):
                    self.images.append(os.path.join(other_folder, img_name))
                    self.labels.append(2)
    
    def __len__(self):
        return len(self.images)
    
    def __getitem__(self, idx):
        img = Image.open(self.images[idx]).convert('RGB')
        label = self.labels[idx]
        
        if self.transform:
            img = self.transform(img)
        
        return img, label

# Data augmentation and normalization for training
train_transform = transforms.Compose([
    transforms.RandomResizedCrop(224, scale=(0.5, 1.0)), # try to 
    # transforms.RandomHorizontalFlip(),
    # transforms.RandomVerticalFlip(),
    transforms.RandomRotation(5),
    transforms.RandomInvert(),
    # transforms.RandomPerspective(),
    transforms.ColorJitter(brightness=0.2, contrast=0.2),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
])

# Just resize and normalize for validation
val_transform = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
])

def train_resnet(write_dest):


    m_folders = ["retrain_corpus/iter2/brenda_asm/m", "retrain_corpus/iter2/brenda_asm/m_lowq"]
    mu_folders = ["retrain_corpus/iter2/brenda_asm/mu", "retrain_corpus/iter2/brenda_open/mu_highq", 
                  "retrain_corpus/iter3/wos_wiley/other"] # these are actually all mu
    other_folders = ["retrain_corpus/iter2/brenda_asm/other", 
                     "retrain_corpus/iter2/brenda_open/other"] # removed a few mus
    full_dataset = CharacterDataset(m_folders, mu_folders, other_folders=other_folders, 
                                    transform=train_transform,
                                    m_limits=[5000,5000],
                                    mu_limits=[5000,5000,5000])

    # Split dataset
    train_size = int(0.8 * len(full_dataset))
    val_size = len(full_dataset) - train_size
    train_dataset, val_dataset = torch.utils.data.random_split(full_dataset, [train_size, val_size])

    # Override transform for validation dataset
    val_dataset.dataset.transform = val_transform

    # Create data loaders
    train_loader = DataLoader(train_dataset, batch_size=32, shuffle=True, num_workers=4)
    val_loader = DataLoader(val_dataset, batch_size=32, shuffle=False, num_workers=4)

    # Load pre-trained ResNet model
    model = resnet18(weights=ResNet18_Weights.DEFAULT)
    num_ftrs = model.fc.in_features
    model.fc = torch.nn.Linear(num_ftrs, 3)  # 2 classes: 'm' and 'mu'

    # Move model to GPU if available
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)

    # Loss function and optimizer
    criterion = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    # Training loop
    num_epochs = 10
    from tqdm import tqdm
    for epoch in range(num_epochs):
        model.train()
        running_loss = 0.0
        for inputs, labels in tqdm(train_loader):
            inputs, labels = inputs.to(device), labels.to(device)
            
            optimizer.zero_grad()
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()
            
            running_loss += loss.item()
        
        # Validation
        model.eval()
        correct = 0
        total = 0
        with torch.no_grad():
            for inputs, labels in val_loader:
                inputs, labels = inputs.to(device), labels.to(device)
                outputs = model(inputs)
                _, predicted = torch.max(outputs.data, 1)
                total += labels.size(0)
                correct += (predicted == labels).sum().item()
        
        print(f'Epoch {epoch+1}/{num_epochs}, Loss: {running_loss/len(train_loader):.4f}, '
            f'Validation Accuracy: {100 * correct / total:.2f}%')
    # save to checkpoint
    torch.save(model.state_dict(), write_dest)


def run_validation(model):
    # Define dataset folders
    m_folders = ["retrain_corpus/iter2/brenda_asm/m", "retrain_corpus/iter2/brenda_asm/m_lowq"]
    mu_folders = ["retrain_corpus/iter2/brenda_asm/mu", "retrain_corpus/iter2/brenda_open/mu_highq", 
                  "retrain_corpus/iter3/wos_wiley/other"]
    other_folders = ["retrain_corpus/iter2/brenda_asm/other", 
                     "retrain_corpus/iter2/brenda_open/other"]

    # Load the full dataset with specified folders and transformations
    full_dataset = CharacterDataset(m_folders, mu_folders, other_folders=other_folders, 
                                    transform=val_transform,
                                    m_limits=[5000, 5000],
                                    mu_limits=[5000, 5000, 5000])

    # Split dataset into train and validation sets
    train_size = int(0.8 * len(full_dataset))
    val_size = len(full_dataset) - train_size
    _, val_dataset = torch.utils.data.random_split(full_dataset, [train_size, val_size])

    # Create a validation data loader
    val_loader = DataLoader(val_dataset, batch_size=32, shuffle=False, num_workers=4)

    # Move model to GPU if available
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    
    # Set the model to evaluation mode
    model.eval()

    # Validation loop
    correct = 0
    total = 0
    with torch.no_grad():
        for inputs, labels in val_loader:
            inputs, labels = inputs.to(device), labels.to(device)
            outputs = model(inputs)
            _, predicted = torch.max(outputs.data, 1)
            total += labels.size(0)
            correct += (predicted == labels).sum().item()

    # Calculate and print final validation accuracy
    final_accuracy = 100 * correct / total
    print(f'Final Validation Accuracy: {final_accuracy:.2f}%')



# Function to classify a new image
def classify_image(model, image_path, device='cpu') -> tuple[str, float]:
    """Return (cls, mu_score)"""
    model.eval()
    if isinstance(image_path, str):
        img = Image.open(image_path).convert('RGB')
    else:
        img = image_path
    
    # Check if the image is valid
    if img.size[0] == 0 or img.size[1] == 0:
        print(f"Warning: Image at {image_path} has zero width or height. Skipping.")
        return "m", 1.0
    img = val_transform(img).unsqueeze(0).to(device)
    
    with torch.no_grad():
        output = model(img)
        probabilities = torch.nn.functional.softmax(output, dim=1)
        # mu_prob = probabilities[0][1].item()
        # adjust for 3 classes
        # mu_prob = probabilities[0][1].item()
        best_label = torch.argmax(probabilities, dim=1).item()
        mu_score = probabilities[0][1].item()
        if best_label == 0:
            return "m", mu_score
        elif best_label == 1:
            return "mu", mu_score
        else:
            return "other", mu_score
        

## START reocr_micromolar

def pixmap_to_PIL(pixmap: pymupdf.Pixmap) -> PILImage:
    """
    Convert a MuPDF pixmap to a PIL Image.
    
    :param pixmap: MuPDF pixmap
    :return: PIL Image
    """
    img = PIL.Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples)
    return img

def yield_all_millimolar(doc, target='mM', allow_lowercase=False, terminating_chars=None, initial_chars=None):
    """
    Note: this is VERY hard-coded for the specific case of 'mM' only.
    Initial chars: a string of characters that can kick off the search. Allows multiple chars to start the search. If None, then defer to target.
    Terminating chars: in addition to a word boundary, a string of characters that can end the search. For instance, 'o' could permit 'mmol' to be a candidate.
    """
    target_len = len(target)
    
    if initial_chars is None:
        initial_chars = target[0] # m
    else:
        assert isinstance(initial_chars, str)
    if terminating_chars is None:
        terminating_chars = ''
    final_chars = target[1:] # M
    
    def is_candidate_good(candidate):
        if len(candidate) < target_len:
            return False
        if allow_lowercase:
            return candidate[0] in initial_chars and candidate[1:].lower() == final_chars.lower()
        return candidate == target
    def is_candidate_on_track(candidate):
        if not candidate:
            return True
        if allow_lowercase:
            return candidate[0] in initial_chars and candidate[1:].lower().startswith(final_chars.lower())
        return target.startswith(candidate)

    for pageno, page in enumerate(doc):
        tp = page.get_textpage(flags=pymupdf.TEXTFLAGS_TEXT) # type: pymupdf.TextPage
        ctr = 0
        for block in tp.extractRAWDICT()['blocks']:
            if block['type'] != 0:
                continue
            for line in block['lines']:
                # we must merge spans when doing this
                angle = 0
                if line["dir"] != (1, 0):
                    if line["dir"] == (0, 1):
                        angle = 90
                    elif line["dir"] == (-1, 0):
                        # print("Unexpected line direction:", line["dir"])
                        angle = 180
                    elif line["dir"] == (0, -1):
                        # print("Unexpected line direction:", line["dir"])
                        angle = 270
                
                m_rect = None
                rect = None
                candidate = ""
                prev_c = None
                
                for span in line['spans']:
                    i = 0
                    while i < len(span['chars']):
                        char = span['chars'][i]

                        # we really only care about the rect for the "m" of mM, because this character might be actually "micro".
                        if not candidate and char['c'] in initial_chars:
                            # then start caring
                            # require tho that it is preceded by a \b
                            if prev_c is None or not prev_c.isalnum():
                                m_rect = pymupdf.Rect(char['bbox'])
                                rect = pymupdf.Rect(char['bbox'])
                                candidate = char['c']
                        elif candidate:
                            candidate += char['c']
                            rect = rect | pymupdf.Rect(char['bbox'])
                            if is_candidate_good(candidate):
                                # also require that it is followed by a \b or 'o'
                                subsequent_char = None
                                if i + 1 < len(span['chars']):
                                    subsequent_char = span['chars'][i+1]['c']

                                # check that we are at a word boundary
                                if (
                                    (not subsequent_char)
                                    or (not subsequent_char.isalnum())
                                    or (subsequent_char in terminating_chars)
                                ):
                                    yield pageno, ctr, candidate, m_rect, rect, angle
                                    ctr += 1
                                    candidate = ""
                            elif not is_candidate_on_track(candidate):
                                # we got off track
                                candidate = ""
                        prev_c = char['c']
                        i += 1
                
                # Check for 'mM' at the end of a line
                if candidate and is_candidate_good(candidate):
                    yield pageno, ctr, candidate, m_rect, rect, angle
                    ctr += 1
        
def _iter_concat(a, b):
    yield from a
    yield from b


def obtain_true_m_dataset(root, all_pdfs):
    # sneaky: search for "mean" and capture these m to build a true dataset
    for pdfname in all_pdfs:
        doc = pymupdf.open(f'{root}/{pdfname}.pdf')
        for pageno, ctr, builder, m_rect, rect, angle in yield_all_millimolar(doc, target="more"): # mean, more, much
            page = doc[pageno]
            # Get the page pixmap
            dpi = 288
            
            margin = 2
            wider_rect = pymupdf.Rect(rect.x0 - margin, rect.y0 - margin, rect.x1 + margin, rect.y1 + margin)
            pixmap = page.get_pixmap(dpi=dpi, clip=wider_rect)
            img = pixmap_to_PIL(pixmap)
            # Crop the image to the rectangle
            # Rotate the image
            img = img.rotate(angle, expand=True)
            # Save the image
            img.save(f'letters/true_m/{pdfname}_{pageno}_{ctr}.png')
        doc.close()
        

def obtain_true_mu_dataset(root, all_pdfs, write_dest):
    # sneaky: search for "mean" and capture these m to build a true dataset
    for pdfname in tqdm(all_pdfs):
        try:
            doc = pymupdf.open(f'{root}/{pdfname}.pdf')
        except Exception as e:
            continue
        for pageno, ctr, builder, m_rect, rect, angle in _iter_concat(
                    yield_all_millimolar(doc, target="µM"), # \u00B5, micro sign
                    yield_all_millimolar(doc, target="μM") # \u03BC, greek letter mu
        ): # mean, more, much
            page = doc[pageno]
            # Get the page pixmap
            dpi = 288
            
            margin = 2
            wider_rect = pymupdf.Rect(rect.x0 - margin, rect.y0 - margin, rect.x1 + margin, rect.y1 + margin)
            
            pixmap = page.get_pixmap(dpi=dpi, clip=wider_rect)
            img = pixmap_to_PIL(pixmap)
            
            # if img is empty, skip
            if img.size[0] == 0 or img.size[1] == 0:
                continue
            # Crop the image to the rectangle
            # Rotate the image
            img = img.rotate(angle, expand=True)
            # Save the image
            img.save(f'{write_dest}/{pdfname}_{pageno}_{ctr}.png')
        doc.close()




def dump_images_too(root, all_pdfs, path_to_dest, target="mM", allow_lowercase=True, save_imgs=True, **kwargs):


    if save_imgs:
        m_dir = f"{path_to_dest}/m"
        mu_dir = f"{path_to_dest}/mu"
        other_dir = f"{path_to_dest}/other"
        
        os.makedirs(m_dir, exist_ok=True)
        os.makedirs(mu_dir, exist_ok=True)
        os.makedirs(other_dir, exist_ok=True)
    
    data = []
    # sneaky: search for "mean" and capture these m to build a true dataset
    # dkip_to = 1680
    for i, pdfname in tqdm(enumerate(all_pdfs), total=len(all_pdfs)):
        # if i < dkip_to:
        #     continue
        try:
            doc = pymupdf.open(f'{root}/{pdfname}.pdf')
        except Exception as e:
            print(f"Error opening {pdfname}: {e}")
            continue
        for pageno, ctr, builder, m_rect, rect, angle in yield_all_millimolar(doc, target=target, allow_lowercase=allow_lowercase, **kwargs): # mean, more, much
            
            page = doc[pageno]
            dpi = 288

            # CHANGE: now obtain the full rect
            margin = 2
            wider_rect = pymupdf.Rect(rect.x0 - margin, rect.y0 - margin, rect.x1 + margin, rect.y1 + margin)
            pixmap = page.get_pixmap(dpi=dpi, clip=wider_rect)
            
            img = pixmap_to_PIL(pixmap)
            
            # skip empty images
            if img.size[0] == 0 or img.size[1] == 0:
                continue
            img = img.rotate(angle, expand=True)
            
            # OCR the image
            # real_char = svm_reocr_milli(img)
            real_char, prob = resnet_reocr_milli(img, mu_score=True)
            
            # store the rect of entire mM (rect)
            big_bbox = (rect.x0, rect.y0, rect.x1, rect.y1)
            m_bbox = (m_rect.x0, m_rect.y0, m_rect.x1, m_rect.y1)
            
            # if real_char != 'm': # only save the micros
            data.append((pdfname, pageno, ctr, builder, real_char, prob, angle, *m_bbox, *big_bbox))
            
            if not save_imgs:
                continue
    
            if real_char == 'm':
                img.save(f'{m_dir}/{pdfname}_{pageno}_{ctr}.png')
            elif real_char == 'mu':
                img.save(f'{mu_dir}/{pdfname}_{pageno}_{ctr}.png')
            else:
                img.save(f'{other_dir}/{pdfname}_{pageno}_{ctr}.png')
        doc.close()
    
    df = pl.DataFrame(data, 
                      orient='row',
                      schema=['pdfname', 'pageno', 'ctr', 'orig_char', 'real_char', 'confidence', 'angle', 'letter_x0', 'letter_y0', 'letter_x1', 'letter_y1', 'x0', 'y0', 'x1', 'y1'])
    return df


resnet = None
def resnet_reocr_milli(img: PILImage, mu_score=True, model_path='zpreprocessing/reocr/resnet18-remicro-iter3.pth') -> str:
    global resnet
    if resnet is None:
        import torch
        from torchvision.models import resnet18, ResNet18_Weights
        model = resnet18(weights=ResNet18_Weights.DEFAULT)
        num_ftrs = model.fc.in_features
        model.fc = torch.nn.Linear(num_ftrs, 3)
        model.load_state_dict(torch.load(model_path, weights_only=True))
        model.eval()
        resnet = model
    out, prob = classify_image(resnet, img, device='cpu')
    if mu_score:
        return out, prob
    if out == 'mu' and prob > 0.996: # very strict requirements for mu
        return 'mu', prob
    return 'm', 1-prob
    
    
def reocr_all_mM(root, all_pdfs, allow_lowercase=True):
    return dump_images_too(root, all_pdfs, None, allow_lowercase=allow_lowercase, save_imgs=False)
            
          
def script1_5():
    # toplevel = 'topoff'
    # suffix = 'hindawi' # local_shim
    root = rf'C:\conjunct\tmp\eval\arctic'
    all_pdfs = set()
    for pdfname in os.listdir(root):
        if pdfname.endswith(".pdf"):
            realname = pdfname[:-4]
            all_pdfs.add(realname)
    
    initial_chars = '\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u0011\u0012\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f'
    df = dump_images_too(root, all_pdfs, 'zpreprocessing/reocr/unicode/dev', target='mM', initial_chars=initial_chars,
                         allow_lowercase=True, save_imgs=True)
    
    df.to_parquet(rf'zpreprocessing/reocr/unicode/unicode_bizarros.csv', index=False, float_format='%.8f')
    exit(0)


if __name__ == '__main__':
    
    script1_5()
    